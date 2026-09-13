package core

import (
	"reflect"
	"testing"
)

// yamlTagged walks a type and reports every struct field carrying a yaml tag,
// as "Type.Field".
func yamlTagged(typ reflect.Type, seen map[reflect.Type]bool, out *[]string) {
	for typ.Kind() == reflect.Ptr || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Map {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct || seen[typ] {
		return
	}
	seen[typ] = true
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if _, ok := f.Tag.Lookup("yaml"); ok {
			*out = append(*out, typ.Name()+"."+f.Name)
		}
		yamlTagged(f.Type, seen, out)
	}
}

// The config file is parsed through yamlFileConfig alone, with unknown keys
// refused. ServerConfig and the types it nests are never decoded from YAML,
// so a yaml tag on any of them is a setting that looks configurable and is
// not: it names a key the parser has never heard of. The runtime structs
// carried eighty-odd such tags for the life of the codebase.
func TestRuntimeConfigIsNotAYAMLTarget(t *testing.T) {
	var tagged []string
	yamlTagged(reflect.TypeOf(ServerConfig{}), map[reflect.Type]bool{}, &tagged)
	if len(tagged) > 0 {
		t.Errorf("yaml tags on runtime config fields, which nothing decodes: %v", tagged)
	}
}
