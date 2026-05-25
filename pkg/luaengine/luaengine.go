// Package luaengine provides a Lua scripting engine for defining custom metrics based on Neo4j queries.
package luaengine

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/prometheus/client_golang/prometheus"
	lua "github.com/yuin/gopher-lua"
)

// Engine manages Lua script execution for custom metrics.
type Engine struct {
	dir     string
	scripts []*lua.LFunction
}

// New creates a Lua engine, loading all .lua files from dir.
func New(dir string) (*Engine, error) {
	e := &Engine{dir: dir}
	if dir == "" {
		return e, nil
	}
	if err := e.loadScripts(); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *Engine) loadScripts() error {
	entries, err := os.ReadDir(e.dir)
	if err != nil {
		return fmt.Errorf("reading lua dir %s: %w", e.dir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".lua" {
			continue
		}
		path := filepath.Join(e.dir, entry.Name())
		slog.Info("loading lua script", "file", path)

		ls := lua.NewState()
		defer ls.Close()

		if err := ls.DoFile(path); err != nil {
			slog.Warn("failed to load lua script", "file", path, "err", err)
			continue
		}

		// Each file's main chunk is treated as a callable function.
		// We store the compiled function for re-execution.
		prototype := ls.GetGlobal(entry.Name())
		if fn, ok := prototype.(*lua.LFunction); ok {
			e.scripts = append(e.scripts, fn)
		}
	}
	return nil
}

// Execute runs all loaded Lua scripts with the given Neo4j query function and metric recorder.
func (e *Engine) Execute(_ context.Context, queryFn func(string) ([]map[string]any, error), gatherer chan<- prometheus.Metric) {
	if len(e.scripts) == 0 {
		return
	}

	for i, fn := range e.scripts {
		ls := lua.NewState()
		// Set up Go bindings
		ls.SetGlobal("neo4j_query", ls.NewFunction(func(l *lua.LState) int {
			cypher := l.CheckString(1)
			records, err := queryFn(cypher)
			if err != nil {
				l.RaiseError("neo4j query error: %s", err.Error())
				return 0
			}
			tbl := ls.NewTable()
			for _, rec := range records {
				row := ls.NewTable()
				for k, v := range rec {
					row.RawSetString(k, toLuaValue(v))
				}
				tbl.Append(row)
			}
			l.Push(tbl)
			return 1
		}))

		ls.SetGlobal("prometheus_record_gauge", ls.NewFunction(func(l *lua.LState) int {
			name := l.CheckString(1)
			value := float64(l.CheckNumber(2))
			nLabels := l.GetTop()
			labels := prometheus.Labels{}
			if nLabels > 2 {
				tbl := l.CheckTable(3)
				tbl.ForEach(func(k, v lua.LValue) {
					labels[k.String()] = v.String()
				})
			}
			m, err := prometheus.NewConstMetric(
				prometheus.NewDesc(name, "Lua-defined metric", nil, labels),
				prometheus.GaugeValue, value,
			)
			if err != nil {
				slog.Warn("lua: invalid metric", "err", err)
				return 0
			}
			gatherer <- m
			return 0
		}))

		// Push the script function and call it
		ls.Push(fn)
		if err := ls.PCall(0, lua.MultRet, nil); err != nil {
			slog.Warn("lua script execution failed", "script_index", i, "err", err)
		}
		ls.Close()
	}
}

func toLuaValue(v any) lua.LValue {
	switch val := v.(type) {
	case nil:
		return lua.LNil
	case int64:
		return lua.LNumber(val)
	case float64:
		return lua.LNumber(val)
	case string:
		return lua.LString(val)
	case bool:
		return lua.LBool(val)
	default:
		return lua.LString(fmt.Sprintf("%v", val))
	}
}
