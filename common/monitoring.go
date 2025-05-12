package common

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

type contextKey string

const requestIDKey contextKey = "x-request-id"
const requestIDHeader = "x-request-id"

func GetRequestID(ctx context.Context) string {
	if requestID, ok := ctx.Value(requestIDKey).(string); ok {
		return requestID
	}
	return ""
}

func GetOrGenerateRequestIDAtServer(ctx context.Context) (context.Context, string) {
	md, _ := metadata.FromIncomingContext(ctx)
	var requestID string
	if ids := md.Get(requestIDHeader); len(ids) > 0 {
		requestID = ids[0]
	} else {
		requestID = uuid.New().String()
		ctx = context.WithValue(ctx, requestIDKey, requestID)
	}
	return ctx, requestID
}

func SetClientRequestID(ctx context.Context) (context.Context, string) {
	md, ok := metadata.FromOutgoingContext(ctx)
	var requestID string
	if ok {
		if ids := md.Get(requestIDHeader); len(ids) > 0 {
			requestID = ids[0]
		}
	}
	if requestID == "" {
		requestID = GetRequestID(ctx)
		if requestID == "" {
			requestID = uuid.New().String()
		}
	}
	md = metadata.Pairs(requestIDHeader, requestID)
	ctx = metadata.NewOutgoingContext(ctx, md)
	return ctx, requestID
}
