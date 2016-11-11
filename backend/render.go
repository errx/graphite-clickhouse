package backend

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/uber-go/zap"
)

type Point struct {
	Metric    string
	Time      int64
	Value     float64
	Timestamp int64 // keep max if metric and time equal on two points
}

type Points []Point

func (s Points) Len() int      { return len(s) }
func (s Points) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

type ByKey struct{ Points }

func (s ByKey) Less(i, j int) bool {
	c := strings.Compare(s.Points[i].Metric, s.Points[j].Metric)

	switch c {
	case -1:
		return true
	case 1:
		return false
	case 0:
		return s.Points[i].Time < s.Points[j].Time
	}

	return false
}

type RenderHandler struct {
	config *Config
}

func (h *RenderHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := Logger(r.Context())
	target := r.URL.Query().Get("target")

	if strings.IndexByte(target, '\'') > -1 { // sql injection dumb fix
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	var prefix string
	var err error

	if h.config.ClickHouse.ExtraPrefix != "" {
		prefix, target, err = RemoveExtraPrefix(h.config.ClickHouse.ExtraPrefix, target)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if target == "" {
			h.Reply(w, r, make([]Point, 0), 0, 0, "")
			return
		}
	}

	fromTimestamp, err := strconv.ParseInt(r.URL.Query().Get("from"), 10, 32)
	if err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	untilTimestamp, err := strconv.ParseInt(r.URL.Query().Get("until"), 10, 32)
	if err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	dateWhere := fmt.Sprintf(
		"(Date >='%s' AND Date <= '%s' AND Time >= %d AND Time <= %d)",
		time.Unix(fromTimestamp, 0).Format("2006-01-02"),
		time.Unix(untilTimestamp, 0).Format("2006-01-02"),
		fromTimestamp,
		untilTimestamp,
	)

	var pathWhere string

	if hasWildcard(target) {
		// Search in small index table first
		treeWhere := makeWhere(target, true)
		if treeWhere == "" {
			http.Error(w, "Bad or unsupported query", http.StatusBadRequest)
			return
		}

		treeData, err := Query(
			r.Context(),
			h.config.ClickHouse.Url,
			fmt.Sprintf("SELECT Path FROM %s WHERE %s GROUP BY Path", h.config.ClickHouse.TreeTable, treeWhere),
			h.config.ClickHouse.TreeTimeout.Value(),
		)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		listBuf := bytes.NewBuffer(nil)
		first := true
		for _, p := range strings.Split(string(treeData), "\n") {
			if p == "" {
				continue
			}

			if !first {
				listBuf.Write([]byte{','})
			}
			first = false

			listBuf.WriteString("'" + p + "'") // SQL-Injection
		}

		if listBuf.Len() == 0 {
			h.Reply(w, r, make([]Point, 0), 0, 0, "")
			return
		}

		pathWhere = fmt.Sprintf(
			"Path IN (%s)",
			string(listBuf.Bytes()),
		)

		// pathWhere = fmt.Sprintf(
		// 	"Path IN (SELECT Path FROM %s WHERE %s)",
		// 	h.config.ClickHouse.DataTable,
		// )
		// pathWhere = makeWhere(target, false)
	} else {
		pathWhere = fmt.Sprintf("Path = '%s'", target)
	}

	// @TODO: change format to RowBinary
	query := fmt.Sprintf(
		`
		SELECT 
			Path, Time, Value, Timestamp
		FROM %s 
		WHERE (%s) AND (%s)
		FORMAT RowBinary
		`,
		h.config.ClickHouse.DataTable,
		pathWhere,
		dateWhere,
	)

	data, err := Query(
		r.Context(),
		h.config.ClickHouse.Url,
		query,
		h.config.ClickHouse.DataTimeout.Value(),
	)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	parseStart := time.Now()

	buf := bytes.NewBuffer(data)

	b := make([]byte, 1024*1024)
	points := make([]Point, 0)

	for {
		namelen, err := binary.ReadUvarint(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		_, err = buf.Read(b[:namelen])
		if err != nil {
			break
		}
		name := string(b[:namelen])

		_, err = buf.Read(b[:4])
		if err != nil {
			break
		}
		time := binary.LittleEndian.Uint32(b[:4])

		_, err = buf.Read(b[:8])
		if err != nil {
			break
		}
		value := math.Float64frombits(binary.LittleEndian.Uint64(b[:8]))

		_, err = buf.Read(b[:4])
		if err != nil {
			break
		}
		timestamp := binary.LittleEndian.Uint32(b[:4])

		points = append(points, Point{
			Metric:    name,
			Time:      int64(time),
			Value:     value,
			Timestamp: int64(timestamp),
		})
	}

	if err != nil && err == io.EOF {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = nil

	logger.Debug("parse", zap.Duration("time_ns", time.Since(parseStart)))

	sortStart := time.Now()
	sort.Sort(ByKey{points})
	logger.Debug("sort", zap.Duration("time_ns", time.Since(sortStart)))

	points = PointsUniq(points)

	// pp.Println(points)
	h.Reply(w, r, points, fromTimestamp, untilTimestamp, prefix)
}

func (h *RenderHandler) Reply(w http.ResponseWriter, r *http.Request, points []Point, from, until int64, prefix string) {
	start := time.Now()
	switch r.URL.Query().Get("format") {
	case "pickle":
		h.ReplyPickle(w, r, points, from, until, prefix)
	case "protobuf":
		h.ReplyProtobuf(w, r, points, from, until, prefix)
	}
	Logger(r.Context()).Debug("reply", zap.Duration("time_ns", time.Since(start)))
}

func (h *RenderHandler) ReplyPickle(w http.ResponseWriter, r *http.Request, points []Point, from, until int64, prefix string) {
	var rollupTime time.Duration
	var pickleTime time.Duration

	defer func() {
		Logger(r.Context()).Debug("rollup", zap.Duration("time_ns", rollupTime))
		Logger(r.Context()).Debug("pickle", zap.Duration("time_ns", pickleTime))
	}()

	if len(points) == 0 {
		w.Write(PickleEmptyList)
		return
	}

	writer := bufio.NewWriterSize(w, 1024*1024)
	p := NewPickler(writer)
	defer writer.Flush()

	p.List()

	writeMetric := func(points []Point) {
		rollupStart := time.Now()
		points, step := h.config.Rollup.RollupMetric(points)
		rollupTime += time.Since(rollupStart)

		pickleStart := time.Now()
		p.Dict()

		p.String("name")
		if prefix != "" {
			p.String(prefix + "." + points[0].Metric)
		} else {
			p.String(points[0].Metric)
		}
		p.SetItem()

		p.String("step")
		p.Uint32(uint32(step))
		p.SetItem()

		start := from - (from % step)
		if start < from {
			start += step
		}
		end := until - (until % step)
		last := start - step

		p.String("values")
		p.List()
		for _, point := range points {
			if point.Time < from || point.Time > until {
				continue
			}

			if point.Time > last+step {
				p.AppendNulls(int(((point.Time - last) / step) - 1))
			}

			p.AppendFloat64(point.Value)

			last = point.Time
		}

		if end > last {
			p.AppendNulls(int((end - last) / step))
		}
		p.SetItem()

		p.String("start")
		p.Uint32(uint32(start))
		p.SetItem()

		p.String("end")
		p.Uint32(uint32(end))
		p.SetItem()

		p.Append()
		pickleTime += time.Since(pickleStart)
	}

	// group by Metric
	var i, n int
	// i - current position of iterator
	// n - position of the first record with current metric
	l := len(points)

	for i = 1; i < l; i++ {
		if points[i].Metric != points[n].Metric {
			writeMetric(points[n:i])
			n = i
			continue
		}
	}
	writeMetric(points[n:i])

	p.Stop()
}

func (h *RenderHandler) ReplyProtobuf(w http.ResponseWriter, r *http.Request, points []Point, from, until int64, prefix string) {
	if len(points) == 0 {
		return
	}

	/*
		message FetchResponse {
		    required string name = 1;
		    required int32 startTime = 2;
		    required int32 stopTime = 3;
		    required int32 stepTime = 4;
		    repeated double values = 5;
		    repeated bool isAbsent = 6;
		}

		message MultiFetchResponse {
		    repeated FetchResponse metrics = 1;
		}
	*/

	writeMetric := func(points []Point) {
		points, step := h.config.Rollup.RollupMetric(points)

		var name string

		if prefix != "" {
			name = prefix + "." + points[0].Metric
		} else {
			name = points[0].Metric
		}

		buf := bytes.NewBuffer(nil)

		// name
		buf.Write(ZipperFetchResponseNameTag)
		ProtobufWriteVarint(buf, uint64(len(name)))
		buf.Write([]byte(name))

		// step
		buf.Write(ZipperFetchResponseStepTimeTag)
		ProtobufWriteVarint(buf, uint64(step))

		start := from - (from % step)
		if start < from {
			start += step
		}
		end := until - (until % step)
		last := start - step

		for _, point := range points {
			if point.Time < from || point.Time > until {
				continue
			}

			if point.Time > last+step {
				ProtobufWriteNullsValues(buf, int(((point.Time-last)/step)-1))
			}

			buf.Write(ZipperFetchResponseValuesTag)
			ProtobufWriteDouble(buf, point.Value)
			buf.Write(ZipperIsPresentValue)

			last = point.Time
		}

		if end > last {
			ProtobufWriteNullsValues(buf, int((end-last)/step))
		}

		buf.Write(ZipperFetchResponseStartTimeTag)
		ProtobufWriteVarint(buf, uint64(start))

		buf.Write(ZipperFetchResponseStopTimeTag)
		ProtobufWriteVarint(buf, uint64(end))

		w.Write(ZipperMultiFetchResponseMetricsTag)
		ProtobufWriteVarint(w, uint64(buf.Len()))
		w.Write(buf.Bytes())
	}

	// group by Metric
	var i, n int
	// i - current position of iterator
	// n - position of the first record with current metric
	l := len(points)

	for i = 1; i < l; i++ {
		if points[i].Metric != points[n].Metric {
			writeMetric(points[n:i])
			n = i
			continue
		}
	}
	writeMetric(points[n:i])
}
func NewRenderHandler(config *Config) *RenderHandler {
	return &RenderHandler{
		config: config,
	}
}