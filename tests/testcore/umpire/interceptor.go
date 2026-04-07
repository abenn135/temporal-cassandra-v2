package umpire

import (
	"context"

	"google.golang.org/grpc"
)

// FaultFunc is called before each gRPC call executes.
// Return nil to proceed normally.
// Return non-nil error to fail the call with that error.
// Block to hold the call (e.g. wait on a channel/context).
type FaultFunc func(ctx context.Context, method string, req any) error

// ObserveFunc is called after each gRPC call completes (or is failed by FaultFunc).
type ObserveFunc func(ctx context.Context, method string, req, resp any, err error)

// Interceptor wraps gRPC calls with pluggable fault injection and observation hooks.
// Both hooks are optional (nil-safe).
type Interceptor struct {
	fault   FaultFunc
	observe ObserveFunc
}

// NewInterceptor creates a new Interceptor with the given hooks.
// Either hook may be nil.
func NewInterceptor(fault FaultFunc, observe ObserveFunc) *Interceptor {
	return &Interceptor{
		fault:   fault,
		observe: observe,
	}
}

// UnaryServerInterceptor returns a gRPC server-side unary interceptor.
func (i *Interceptor) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if i.fault != nil {
			if err := i.fault(ctx, info.FullMethod, req); err != nil {
				if i.observe != nil {
					i.observe(ctx, info.FullMethod, req, nil, err)
				}
				return nil, err
			}
		}
		resp, err := handler(ctx, req)
		if i.observe != nil {
			i.observe(ctx, info.FullMethod, req, resp, err)
		}
		return resp, err
	}
}

// UnaryClientInterceptor returns a gRPC client-side unary interceptor.
func (i *Interceptor) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if i.fault != nil {
			if err := i.fault(ctx, method, req); err != nil {
				if i.observe != nil {
					i.observe(ctx, method, req, nil, err)
				}
				return err
			}
		}
		err := invoker(ctx, method, req, reply, cc, opts...)
		if i.observe != nil {
			i.observe(ctx, method, req, reply, err)
		}
		return err
	}
}
