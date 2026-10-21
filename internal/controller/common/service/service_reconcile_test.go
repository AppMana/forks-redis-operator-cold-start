package service

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type failingServiceClient struct {
	client.Client
	failure   error
	operation string
}

func (c failingServiceClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if c.operation == "get" {
		return c.failure
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c failingServiceClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if c.operation == "create" {
		return c.failure
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c failingServiceClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if c.operation == "update" {
		return c.failure
	}
	return c.Client.Update(ctx, obj, opts...)
}

func TestServiceReconcileReturnsAPIErrorsWithoutPanic(t *testing.T) {
	for _, operation := range []string{"get", "create", "update"} {
		t.Run(operation, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, corev1.AddToScheme(scheme))
			expected := corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "redis", Namespace: "test"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 6379}}}}
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if operation == "update" {
				builder.WithObjects(&corev1.Service{ObjectMeta: expected.ObjectMeta})
			}
			failure := errors.New("API unavailable")
			c := failingServiceClient{Client: builder.Build(), failure: failure, operation: operation}
			require.NotPanics(t, func() {
				_, err := Reconcile(context.Background(), c, expected, nil)
				require.ErrorIs(t, err, failure)
			})
		})
	}
}
