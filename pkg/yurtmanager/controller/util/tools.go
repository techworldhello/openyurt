/*
Copyright 2020 The OpenYurt Authors.
Copyright 2019 The Kruise Authors.
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/utils/integer"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	controllerimpl "github.com/openyurtio/openyurt/pkg/yurtmanager/controller/internal/controller"
)

// SlowStartBatch tries to call the provided function a total of 'count' times,
// starting slow to check for errors, then speeding up if calls succeed.
//
// It groups the calls into batches, starting with a group of initialBatchSize.
// Within each batch, it may call the function multiple times concurrently with its index.
//
// If a whole batch succeeds, the next batch may get exponentially larger.
// If there are any failures in a batch, all remaining batches are skipped
// after waiting for the current batch to complete.
//
// It returns the number of successful calls to the function.
func SlowStartBatch(count int, initialBatchSize int, fn func(index int) error) (int, error) {
	remaining := count
	successes := 0
	index := 0
	for batchSize := integer.IntMin(remaining, initialBatchSize); batchSize > 0; batchSize = integer.IntMin(2*batchSize, remaining) {
		errCh := make(chan error, batchSize)
		var wg sync.WaitGroup
		wg.Add(batchSize)
		for i := 0; i < batchSize; i++ {
			go func(idx int) {
				defer wg.Done()
				if err := fn(idx); err != nil {
					errCh <- err
				}
			}(index)
			index++
		}
		wg.Wait()
		curSuccesses := batchSize - len(errCh)
		successes += curSuccesses
		close(errCh)
		if len(errCh) > 0 {
			errs := make([]error, 0)
			for err := range errCh {
				errs = append(errs, err)
			}
			return successes, utilerrors.NewAggregate(errs)
		}
		remaining -= batchSize
	}
	return successes, nil
}

func NewNoReconcileController(name string, mgr manager.Manager, options controller.Options) (*controllerimpl.Controller[reconcile.Request], error) {
	if len(name) == 0 {
		return nil, fmt.Errorf("must specify Name for Controller")
	}

	if options.CacheSyncTimeout == 0 {
		options.CacheSyncTimeout = 2 * time.Minute
	}

	if options.RateLimiter == nil {
		options.RateLimiter = workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]()
	}

	log := mgr.GetLogger().WithValues(
		"controller", name,
	)

	// Create controller with dependencies set
	c := &controllerimpl.Controller[reconcile.Request]{
		NewQueue: func(controllerName string, rateLimiter workqueue.TypedRateLimiter[reconcile.Request]) workqueue.TypedRateLimitingInterface[reconcile.Request] {
			return workqueue.NewTypedRateLimitingQueueWithConfig(rateLimiter, workqueue.TypedRateLimitingQueueConfig[reconcile.Request]{Name: controllerName})
		},
		CacheSyncTimeout: options.CacheSyncTimeout,
		Name:             name,
		RateLimiter:      options.RateLimiter,
		RecoverPanic:     options.RecoverPanic,
		LogConstructor: func(req *reconcile.Request) logr.Logger {
			log := log
			if req != nil {
				log = log.WithValues(
					"object", klog.KRef(req.Namespace, req.Name),
					"namespace", req.Namespace, "name", req.Name,
				)
			}
			return log
		},
	}

	if err := mgr.Add(c); err != nil {
		return c, err
	}
	return c, nil
}
