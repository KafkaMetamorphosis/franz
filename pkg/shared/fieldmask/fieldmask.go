// Package fieldmask applies an AIP-203 `google.protobuf.FieldMask` to a resource
// message the way 003.1 ("Partial updates") mandates:
//
//   - only fields named in the mask are touched; everything else is left as-is;
//   - an empty (or nil) mask is rejected with INVALID_ARGUMENT;
//   - a masked `map` or `repeated` field is replaced wholesale;
//   - `name` is immutable and may not appear in the mask; neither may the
//     server-assigned fields `frn`, `created_at`, `updated_at`.
//
// Only top-level field paths are supported — every Franz Update request masks
// direct fields of the resource.
package fieldmask

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
)

// immutable is the set of field paths that can never be masked.
var immutable = map[string]bool{
	"name":        true,
	"frn":         true,
	"created_at":  true,
	"updated_at":  true,
	"update_mask": true,
}

// Validate checks mask against the descriptor of msg. It returns an
// INVALID_ARGUMENT domain error (field "update_mask") when the mask is empty,
// names an unknown field, or names an immutable field.
func Validate(mask *fieldmaskpb.FieldMask, msg proto.Message) error {
	if mask == nil || len(mask.GetPaths()) == 0 {
		return errs.InvalidField("update_mask", "must not be empty")
	}
	fields := msg.ProtoReflect().Descriptor().Fields()
	for _, path := range mask.GetPaths() {
		if immutable[path] {
			return errs.InvalidField("update_mask", "field "+path+" is immutable and cannot be updated")
		}
		if fieldByName(fields, path) == nil {
			return errs.InvalidField("update_mask", "unknown field "+path)
		}
	}
	return nil
}

// Apply validates the mask, then copies every masked field from src onto dst.
// A masked field that is unset on src is cleared on dst (so the client always
// sends the full desired value — 003.1). src and dst must be the same message
// type; dst is mutated in place.
func Apply(mask *fieldmaskpb.FieldMask, src, dst proto.Message) error {
	if err := Validate(mask, dst); err != nil {
		return err
	}
	srcR := src.ProtoReflect()
	dstR := dst.ProtoReflect()
	if srcR.Descriptor() != dstR.Descriptor() {
		return errs.Internalf("fieldmask: src and dst are different message types")
	}
	fields := dstR.Descriptor().Fields()
	for _, path := range mask.GetPaths() {
		fd := fieldByName(fields, path)
		if fd == nil {
			continue // unreachable after Validate
		}
		if srcR.Has(fd) {
			dstR.Set(fd, srcR.Get(fd))
		} else {
			dstR.Clear(fd)
		}
	}
	return nil
}

// CanonicalPaths validates the mask, then returns its paths as proto field
// names (resolving any lowerCamel JSON-name aliases). Handlers switch on the
// result to build a typed update input.
func CanonicalPaths(mask *fieldmaskpb.FieldMask, msg proto.Message) ([]string, error) {
	if err := Validate(mask, msg); err != nil {
		return nil, err
	}
	fields := msg.ProtoReflect().Descriptor().Fields()
	paths := make([]string, 0, len(mask.GetPaths()))
	for _, p := range mask.GetPaths() {
		paths = append(paths, string(fieldByName(fields, p).Name()))
	}
	return paths, nil
}

// fieldByName resolves a mask path against both the proto field name and the
// lowerCamel JSON name.
func fieldByName(fields protoreflect.FieldDescriptors, path string) protoreflect.FieldDescriptor {
	if fd := fields.ByName(protoreflect.Name(path)); fd != nil {
		return fd
	}
	return fields.ByJSONName(path)
}
