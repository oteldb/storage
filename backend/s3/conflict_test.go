package s3_test

import (
	"context"
	"net/http"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/s3"
)

// failingPutAWS answers every PutObject with a fixed error, so the adapter's translation of an
// SDK error into the conditional-write contract can be driven code by code.
type failingPutAWS struct {
	*fakeAWS

	err error
}

func (f *failingPutAWS) PutObject(context.Context, *awss3.PutObjectInput, ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	return nil, f.err
}

// responseErr reproduces the shape the S3 client actually hands back for an unmodeled error: a
// *smithy.GenericAPIError carrying the <Code> from the response body, wrapped in the transport's
// ResponseError. Only errors.As through that wrapper reaches the API error.
func responseErr(status int, code string) error {
	return &awshttp.ResponseError{
		ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
			Err:      &smithy.GenericAPIError{Code: code, Message: code},
		},
		RequestID: "req",
	}
}

func TestAWSConditionalPutConflictCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		conflict bool // condition lost: (false, nil), not an error
	}{
		{"precondition_failed", responseErr(http.StatusPreconditionFailed, "PreconditionFailed"), true},
		{"conditional_request_conflict", responseErr(http.StatusConflict, "ConditionalRequestConflict"), true},
		{"operation_aborted", responseErr(http.StatusConflict, "OperationAborted"), false},
		{"internal_error", responseErr(http.StatusInternalServerError, "InternalError"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			store := s3.NewAWS(&failingPutAWS{fakeAWS: newFakeAWS(), err: tt.err}, "bucket")

			created, err := store.PutObjectIfAbsent(ctx, "k", []byte("v"))
			if tt.conflict {
				require.NoError(t, err, "a lost conditional put is not an error")
				assert.False(t, created)
			} else {
				require.Error(t, err)
			}

			_, ok, err := store.PutObjectIfVersion(ctx, "k", []byte("v"), "etag")
			if tt.conflict {
				require.NoError(t, err, "a lost conditional put is not an error")
				assert.False(t, ok)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// TestAWSConditionalPutConflictRebases pins the consequence the codes carry: a 409 must reach the
// committer as a lost race it can rebase on, not as a backend failure that aborts the flush.
func TestAWSConditionalPutConflictRebases(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	err := responseErr(http.StatusConflict, "ConditionalRequestConflict")
	b := s3.New(s3.NewAWS(&failingPutAWS{fakeAWS: newFakeAWS(), err: err}, "bucket"), "oteldb/")

	_, ok, casErr := b.CompareAndSwap(ctx, "index", "etag", []byte("v"))
	require.NoError(t, casErr)
	assert.False(t, ok, "the committer reloads and retries instead of failing the commit")
}
