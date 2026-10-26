package cron

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServerEndpoint(t *testing.T) {
	t.Run("default empty endpoint", func(t *testing.T) {
		srv := NewServer()
		endpoint, err := srv.Endpoint()
		require.NoError(t, err)
		require.NotNil(t, endpoint)
		require.Empty(t, endpoint.String())
	})

	t.Run("custom endpoint", func(t *testing.T) {
		endpoint, err := url.Parse("cron://jobs/settlement")
		require.NoError(t, err)

		srv := NewServer(WithEndpoint(endpoint))
		got, err := srv.Endpoint()
		require.NoError(t, err)
		require.Equal(t, endpoint.String(), got.String())
	})
}

func TestServerStartTimerJob(t *testing.T) {
	srv := NewServer()

	var counter atomic.Int32
	entryID, err := srv.StartTimerJob("@every 1s", func() {
		counter.Add(1)
	})
	require.NoError(t, err)
	require.NotZero(t, entryID)
	require.Equal(t, 1, srv.GetJobCount())

	spec, ok := srv.JobSpec(entryID)
	require.True(t, ok)
	require.Equal(t, "@every 1s", spec)

	require.NoError(t, srv.Start(context.Background()))
	t.Cleanup(func() {
		_ = srv.Stop(context.Background())
	})

	require.Eventually(t, func() bool {
		return counter.Load() > 0
	}, 2500*time.Millisecond, 100*time.Millisecond)
}

func TestServerAcceptsFiveFieldSpec(t *testing.T) {
	srv := NewServer()
	entryID, err := srv.StartTimerJob("*/5 * * * *", func() {})
	require.NoError(t, err)
	require.NotZero(t, entryID)
}

func TestStopTimerJobAndStopAllJobs(t *testing.T) {
	srv := NewServer()

	entryID1, err := srv.StartTimerJob("@every 1s", func() {})
	require.NoError(t, err)
	entryID2, err := srv.StartTimerJob("*/5 * * * *", func() {})
	require.NoError(t, err)

	require.Equal(t, 2, srv.GetJobCount())

	srv.StopTimerJob(entryID1)
	require.Equal(t, 1, srv.GetJobCount())

	_, ok := srv.JobSpec(entryID1)
	require.False(t, ok)
	_, ok = srv.JobSpec(entryID2)
	require.True(t, ok)

	srv.StopAllJobs()
	require.Zero(t, srv.GetJobCount())
}

func TestServerGracefulStopWaitsForRunningJobs(t *testing.T) {
	srv := NewServer()

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once

	_, err := srv.StartTimerJob("@every 1s", func() {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	})
	require.NoError(t, err)

	require.NoError(t, srv.Start(context.Background()))
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		_ = srv.Stop(context.Background())
	})

	select {
	case <-entered:
	case <-time.After(2500 * time.Millisecond):
		t.Fatal("cron job did not start in time")
	}

	stopDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		stopDone <- srv.Stop(ctx)
	}()

	select {
	case err := <-stopDone:
		t.Fatalf("stop returned before job finished: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(release) })

	select {
	case err := <-stopDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("stop did not return after job finished")
	}
}

func TestStartTimerJobValidateInput(t *testing.T) {
	srv := NewServer()

	_, err := srv.StartTimerJob("", func() {})
	require.Error(t, err)

	_, err = srv.StartTimerJob("@every 1s", nil)
	require.Error(t, err)
}
