package postgres

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/stephanfeb/go-ricochet/internal/storage"
)

// validFieldName matches safe JSONB field names (alphanumeric, underscore, dot, hyphen).
var validFieldName = regexp.MustCompile(`^[a-zA-Z0-9_.\-]+$`)

// comparisonOps maps filter operators to SQL comparison operators.
var comparisonOps = map[string]string{
	"$lt":  "<",
	"$lte": "<=",
	"$gt":  ">",
	"$gte": ">=",
}

// BuildJSONBFilter translates a filter map into a parameterized SQL WHERE clause
// and a slice of arguments. The returned clause does NOT include "WHERE".
//
// Supported operators:
//
//	Equality:     {"field": "value"}          → content @> $N::jsonb
//	$lt/$lte/$gt/$gte: {"field": {"$lt": N}}  → (content->>'field')::numeric < $N
//	$ilike:       {"field": {"$ilike": "%x%"}} → content->>'field' ILIKE $N
//	$contains:    {"field": {"$contains": V}}  → content->'field' @> $N::jsonb
//	$and:         {"$and": [...]}              → (clause1) AND (clause2) AND ...
//	$or:          {"$or": [...]}               → (clause1) OR (clause2) OR ...
//
// Returns ("TRUE", nil, nil) for empty/nil filters.
func BuildJSONBFilter(filter map[string]any, startArgIdx int) (string, []any, error) {
	if len(filter) == 0 {
		return "TRUE", nil, nil
	}

	var clauses []string
	var args []any
	argIdx := startArgIdx

	for key, value := range filter {
		switch key {
		case "$and", "$or":
			arr, ok := value.([]any)
			if !ok {
				return "", nil, fmt.Errorf("%w: operator %s requires an array", storage.ErrInvalidFilter, key)
			}
			if len(arr) == 0 {
				continue
			}

			var subClauses []string
			for _, item := range arr {
				subFilter, ok := item.(map[string]any)
				if !ok {
					return "", nil, fmt.Errorf("%w: operator %s array items must be objects", storage.ErrInvalidFilter, key)
				}
				clause, subArgs, err := BuildJSONBFilter(subFilter, argIdx)
				if err != nil {
					return "", nil, err
				}
				subClauses = append(subClauses, clause)
				args = append(args, subArgs...)
				argIdx += len(subArgs)
			}

			joiner := " AND "
			if key == "$or" {
				joiner = " OR "
			}
			clauses = append(clauses, "("+strings.Join(subClauses, joiner)+")")

		default:
			if !validFieldName.MatchString(key) {
				return "", nil, fmt.Errorf("%w: invalid field name %q", storage.ErrInvalidFilter, key)
			}

			switch v := value.(type) {
			case map[string]any:
				// Operator object: {"$lt": 50, "$ilike": "%x%", ...}
				for op, opVal := range v {
					switch {
					case comparisonOps[op] != "":
						clauses = append(clauses,
							fmt.Sprintf("(content->>'%s')::numeric %s $%d", key, comparisonOps[op], argIdx))
						args = append(args, opVal)
						argIdx++

					case op == "$ilike":
						pattern, ok := opVal.(string)
						if !ok {
							return "", nil, fmt.Errorf("%w: $ilike requires a string value", storage.ErrInvalidFilter)
						}
						clauses = append(clauses,
							fmt.Sprintf("content->>'%s' ILIKE $%d", key, argIdx))
						args = append(args, pattern)
						argIdx++

					case op == "$contains":
						jsonVal, err := json.Marshal(opVal)
						if err != nil {
							return "", nil, fmt.Errorf("marshal $contains value: %w", err)
						}
						clauses = append(clauses,
							fmt.Sprintf("content->'%s' @> $%d::jsonb", key, argIdx))
						args = append(args, string(jsonVal))
						argIdx++

					default:
						return "", nil, fmt.Errorf("%w: unknown operator %s", storage.ErrInvalidFilter, op)
					}
				}

			default:
				// Equality: {"field": "value"} → content @> '{"field": "value"}'::jsonb
				obj := map[string]any{key: value}
				jsonVal, err := json.Marshal(obj)
				if err != nil {
					return "", nil, fmt.Errorf("marshal equality filter: %w", err)
				}
				clauses = append(clauses,
					fmt.Sprintf("content @> $%d::jsonb", argIdx))
				args = append(args, string(jsonVal))
				argIdx++
			}
		}
	}

	if len(clauses) == 0 {
		return "TRUE", nil, nil
	}

	return strings.Join(clauses, " AND "), args, nil
}
