package grpcgateway

import (
	"errors"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
)

func TestToStatusCodes(t *testing.T) {
	cases := []struct {
		err  error
		want codes.Code
	}{
		{errs.Internalf("x"), codes.Internal},
		{errs.Invalidf("x"), codes.InvalidArgument},
		{errs.NotFoundf("x"), codes.NotFound},
		{errs.Existsf("x"), codes.AlreadyExists},
		{errs.Preconditionf("x"), codes.FailedPrecondition},
		{errs.Deniedf("x"), codes.PermissionDenied},
		{errs.Exhaustedf("x"), codes.ResourceExhausted},
		{errors.New("plain"), codes.Internal},
		{nil, codes.OK},
	}
	for _, c := range cases {
		if got := ToStatus(c.err).Code(); got != c.want {
			t.Errorf("ToStatus(%v).Code() = %v, want %v", c.err, got, c.want)
		}
	}
}

func TestToStatusAttachesBadRequest(t *testing.T) {
	err := errs.Invalidf("invalid request").
		AddViolation("name", "too long").
		AddViolation("labels", "reserved prefix")

	st := ToStatus(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("code = %v", st.Code())
	}

	var br *errdetails.BadRequest
	for _, d := range st.Details() {
		if b, ok := d.(*errdetails.BadRequest); ok {
			br = b
		}
	}
	if br == nil {
		t.Fatal("no BadRequest detail attached")
	}
	if len(br.FieldViolations) != 2 {
		t.Fatalf("want 2 field violations, got %d", len(br.FieldViolations))
	}
	if br.FieldViolations[0].Field != "name" || br.FieldViolations[1].Field != "labels" {
		t.Errorf("field violations = %+v", br.FieldViolations)
	}
}

func TestToStatusNoDetailsWithoutViolations(t *testing.T) {
	st := ToStatus(errs.NotFoundf("kafka-cluster %q not found", "east-1"))
	if len(st.Details()) != 0 {
		t.Errorf("expected no details, got %v", st.Details())
	}
}

func TestToErrorRoundTripsThroughStatus(t *testing.T) {
	err := ToError(errs.NotFoundf("nope"))
	if status.Code(err) != codes.NotFound {
		t.Errorf("status.Code = %v", status.Code(err))
	}
	if ToError(nil) != nil {
		t.Error("ToError(nil) should be nil")
	}
}

func TestNonDomainErrorIsOpaque(t *testing.T) {
	st := ToStatus(errors.New("db password is hunter2"))
	if st.Message() == "db password is hunter2" {
		t.Error("internal error message leaked to caller")
	}
}
