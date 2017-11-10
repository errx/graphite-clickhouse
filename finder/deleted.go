package finder

import (
	"context"
	"fmt"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/config"
)

type DeletedFinder struct {
	*BaseFinder
}

func NewDeleted(ctx context.Context, config *config.Config) Finder {
	b := NewBase(ctx, config.ClickHouse.Url, config.ClickHouse.TreeTable, config.ClickHouse.TreeTimeout.Value())
	return &DeletedFinder{b}
}

func (b *DeletedFinder) Execute(query string) (err error) {
	where := b.where(query)

	b.body, err = clickhouse.Query(
		b.ctx,
		b.url,
		fmt.Sprintf("SELECT Path FROM %s WHERE %s GROUP BY Path HAVING argMax(Deleted, Version)==1", b.table, where),
		b.timeout,
	)

	return
}
