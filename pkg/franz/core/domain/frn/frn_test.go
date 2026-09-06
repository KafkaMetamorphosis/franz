package frn

import "testing"

func TestNewPathAndString(t *testing.T) {
	f, err := New("default", TypeAsyncChannel, "orders")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got, want := f.Path(), "default:async-channel:orders"; got != want {
		t.Errorf("Path() = %q, want %q", got, want)
	}
	if got, want := f.String(), "frn:default:async-channel:orders"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
	if f.Realm() != "default" || f.Type() != TypeAsyncChannel || f.Name() != "orders" {
		t.Errorf("accessors wrong: %+v", f)
	}
}

func TestParseFormsRoundTrip(t *testing.T) {
	for _, in := range []string{
		"frn:default:kafka-cluster:east-1",  // default prefix
		"orn:default:kafka-cluster:east-1",  // legacy alias
		"acme:default:kafka-cluster:east-1", // arbitrary prefix (value object doesn't police)
		"default:kafka-cluster:east-1",      // prefix-less path
	} {
		f, err := Parse(in)
		if err != nil {
			t.Fatalf("Parse(%q): %v", in, err)
		}
		if f.Path() != "default:kafka-cluster:east-1" {
			t.Errorf("Parse(%q).Path() = %q", in, f.Path())
		}
	}
}

func TestParsePath(t *testing.T) {
	f, err := ParsePath("default:agent:provisioner-1")
	if err != nil {
		t.Fatalf("ParsePath: %v", err)
	}
	if f.Type() != TypeAgent {
		t.Errorf("type = %v", f.Type())
	}
	if _, err := ParsePath("frn:default:agent:provisioner-1"); err == nil {
		t.Error("ParsePath should reject a prefixed string")
	}
}

func TestParseErrors(t *testing.T) {
	bad := []string{
		"",
		"default:async-channel",                  // too few segments
		"frn:default:async-channel:orders:extra", // too many
		"frn:default:not-a-type:orders",          // unknown type
		"frn:Default:async-channel:orders",       // bad realm slug
		"frn:default:async-channel:Orders",       // bad name
		"frn::async-channel:orders",              // empty realm
	}
	for _, s := range bad {
		if _, err := Parse(s); err == nil {
			t.Errorf("Parse(%q) = nil error, want error", s)
		}
	}
}

func TestZeroValue(t *testing.T) {
	var f FRN
	if !f.IsZero() {
		t.Error("zero FRN should be IsZero")
	}
	if f.String() != "" || f.Path() != "" {
		t.Errorf("zero FRN String()=%q Path()=%q, want empty", f.String(), f.Path())
	}
}

func TestTextMarshaling(t *testing.T) {
	f := MustParse("frn:default:agent:provisioner-1")
	b, err := f.MarshalText()
	if err != nil {
		t.Fatal(err)
	}
	var back FRN
	if err := back.UnmarshalText(b); err != nil {
		t.Fatal(err)
	}
	if back != f {
		t.Errorf("round-trip: %v != %v", back, f)
	}
	if err := back.UnmarshalText([]byte("garbage")); err == nil {
		t.Error("UnmarshalText(garbage) should error")
	}
}

func TestResourceTypeValid(t *testing.T) {
	if !TypeKafkaTopic.Valid() {
		t.Error("kafka-topic should be valid")
	}
	if ResourceType("bogus").Valid() {
		t.Error("bogus should be invalid")
	}
}

func TestValidatePrefix(t *testing.T) {
	for _, ok := range []string{"frn", "orn", "acme", "ab", "x1y2", "sixteencharsxxxx"} {
		if err := ValidatePrefix(ok); err != nil {
			t.Errorf("ValidatePrefix(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{"", "a", "1abc", "Acme", "a-b", "a_b", "toolongprefixname17", "café"} {
		if err := ValidatePrefix(bad); err == nil {
			t.Errorf("ValidatePrefix(%q) = nil, want error", bad)
		}
	}
}

func TestCodecRenderAndParse(t *testing.T) {
	c := MustCodec("acme")
	f := MustParse("frn:default:client:billing")

	if got, want := c.Render(f), "acme:default:client:billing"; got != want {
		t.Errorf("Render = %q, want %q", got, want)
	}

	// accepts configured prefix, aliases, and bare path
	for _, in := range []string{
		"acme:default:client:billing",
		"frn:default:client:billing",
		"orn:default:client:billing",
		"default:client:billing",
	} {
		got, err := c.Parse(in)
		if err != nil {
			t.Fatalf("Codec.Parse(%q): %v", in, err)
		}
		if got != f {
			t.Errorf("Codec.Parse(%q) = %v, want %v", in, got, f)
		}
	}

	// rejects an unrecognised prefix
	if _, err := c.Parse("other:default:client:billing"); err == nil {
		t.Error("Codec.Parse should reject an unrecognised prefix")
	}
}

func TestNewCodecValidatesPrefix(t *testing.T) {
	if _, err := NewCodec("Bad-Prefix"); err == nil {
		t.Error("NewCodec should reject an invalid prefix")
	}
	c, err := NewCodec("")
	if err != nil {
		t.Fatalf("NewCodec(\"\"): %v", err)
	}
	if c.Prefix() != DefaultPrefix {
		t.Errorf("empty prefix -> %q, want %q", c.Prefix(), DefaultPrefix)
	}
}
