package grpcgateway

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
)

// kindToCode maps a domain error Kind onto the gRPC status code mandated by
// 003.1 ("Errors"). grpc-gateway then renders the code as the documented HTTP
// status.
var kindToCode = map[errs.Kind]codes.Code{
	errs.Internal:           codes.Internal,
	errs.InvalidArgument:    codes.InvalidArgument,
	errs.NotFound:           codes.NotFound,
	errs.AlreadyExists:      codes.AlreadyExists,
	errs.FailedPrecondition: codes.FailedPrecondition,
	errs.PermissionDenied:   codes.PermissionDenied,
	errs.ResourceExhausted:  codes.ResourceExhausted,
}

// ToStatus converts any error into a *status.Status. A domain *errs.Error keeps
// its Kind and, when it carries field violations, attaches a
// google.rpc.BadRequest detail (003.1). A non-domain error becomes INTERNAL with
// no details leaked to the caller.
func ToStatus(err error) *status.Status {
	if err == nil {
		return status.New(codes.OK, "")
	}

	domErr, ok := errs.As(err)
	if !ok {
		return status.New(codes.Internal, "internal error")
	}

	code, known := kindToCode[domErr.Kind]
	if !known {
		code = codes.Internal
	}

	st := status.New(code, domErr.Msg)
	if len(domErr.Violations) == 0 {
		return st
	}

	br := &errdetails.BadRequest{}
	for _, v := range domErr.Violations {
		br.FieldViolations = append(br.FieldViolations, &errdetails.BadRequest_FieldViolation{
			Field:       v.Field,
			Description: v.Description,
		})
	}
	if withDetails, dErr := st.WithDetails(br); dErr == nil {
		return withDetails
	}
	return st
}

// ToError is ToStatus(err).Err() — the value a gRPC handler returns.
func ToError(err error) error {
	if err == nil {
		return nil
	}
	return ToStatus(err).Err()
}
