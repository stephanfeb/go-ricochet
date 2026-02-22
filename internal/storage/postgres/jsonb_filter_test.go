package postgres

import (
	"testing"
)

func TestBuildJSONBFilter(t *testing.T) {
	tests := []struct {
		name      string
		filter    map[string]any
		startIdx  int
		wantSQL   string
		wantNArgs int
		wantErr   bool
	}{
		{
			name:      "nil filter",
			filter:    nil,
			startIdx:  1,
			wantSQL:   "TRUE",
			wantNArgs: 0,
		},
		{
			name:      "empty filter",
			filter:    map[string]any{},
			startIdx:  1,
			wantSQL:   "TRUE",
			wantNArgs: 0,
		},
		{
			name:      "equality string",
			filter:    map[string]any{"category": "tools"},
			startIdx:  2,
			wantSQL:   "content @> $2::jsonb",
			wantNArgs: 1,
		},
		{
			name:      "equality number",
			filter:    map[string]any{"active": true},
			startIdx:  1,
			wantSQL:   "content @> $1::jsonb",
			wantNArgs: 1,
		},
		{
			name:     "$lt comparison",
			filter:   map[string]any{"price": map[string]any{"$lt": 50}},
			startIdx: 1,
			wantSQL:  "(content->>'price')::numeric < $1",
		},
		{
			name:     "$gte comparison",
			filter:   map[string]any{"price": map[string]any{"$gte": 10}},
			startIdx: 1,
			wantSQL:  "(content->>'price')::numeric >= $1",
		},
		{
			name:     "$ilike text match",
			filter:   map[string]any{"name": map[string]any{"$ilike": "%drill%"}},
			startIdx: 1,
			wantSQL:  "content->>'name' ILIKE $1",
		},
		{
			name:     "$contains array",
			filter:   map[string]any{"tags": map[string]any{"$contains": "sale"}},
			startIdx: 1,
			wantSQL:  "content->'tags' @> $1::jsonb",
		},
		{
			name:      "invalid field name",
			filter:    map[string]any{"'; DROP TABLE": "x"},
			startIdx:  1,
			wantErr:   true,
		},
		{
			name: "$and compound",
			filter: map[string]any{
				"$and": []any{
					map[string]any{"category": "tools"},
					map[string]any{"price": map[string]any{"$lt": 50}},
				},
			},
			startIdx: 1,
			wantNArgs: 2,
		},
		{
			name: "$or compound",
			filter: map[string]any{
				"$or": []any{
					map[string]any{"category": "tools"},
					map[string]any{"category": "hardware"},
				},
			},
			startIdx: 1,
			wantNArgs: 2,
		},
		{
			name:     "unknown operator",
			filter:   map[string]any{"price": map[string]any{"$unknown": 50}},
			startIdx: 1,
			wantErr:  true,
		},
		{
			name:     "$and with non-array",
			filter:   map[string]any{"$and": "not an array"},
			startIdx: 1,
			wantErr:  true,
		},
		{
			name:     "$ilike with non-string",
			filter:   map[string]any{"name": map[string]any{"$ilike": 42}},
			startIdx: 1,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, args, err := BuildJSONBFilter(tt.filter, tt.startIdx)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (sql=%q)", sql)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.wantSQL != "" && sql != tt.wantSQL {
				t.Errorf("SQL mismatch:\n  got:  %q\n  want: %q", sql, tt.wantSQL)
			}

			if tt.wantNArgs > 0 && len(args) != tt.wantNArgs {
				t.Errorf("args count: got %d, want %d", len(args), tt.wantNArgs)
			}
		})
	}
}
