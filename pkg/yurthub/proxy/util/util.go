/*
Copyright 2022 The OpenYurt Authors.

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
	"context"
	"fmt"
	"mime"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-jose/go-jose/v3/jwt"
	"github.com/munnerz/goautoneg"
	"k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metainternalversionscheme "k8s.io/apimachinery/pkg/apis/meta/internalversion/scheme"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/streaming"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/serviceaccount"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/klog/v2"

	"github.com/openyurtio/openyurt/pkg/yurthub/kubernetes/serializer"
	"github.com/openyurtio/openyurt/pkg/yurthub/metrics"
	"github.com/openyurtio/openyurt/pkg/yurthub/multiplexer"
	"github.com/openyurtio/openyurt/pkg/yurthub/tenant"
	"github.com/openyurtio/openyurt/pkg/yurthub/util"
)

const (
	canCacheHeader          string = "Edge-Cache"
	watchTimeoutMargin      int64  = 15
	getAndListTimeoutReduce int64  = 2
)

var needModifyTimeoutVerb = map[string]bool{
	"get":   true,
	"list":  true,
	"watch": true,
}

// WithIsRequestForPoolScopeMetadata add a mark in context for specifying whether a request is used for list/watching pool scope metadata or not,
// request for pool scope metadata will be handled by multiplexer manager instead forwarding to cloud kube-apiserver.
func WithIsRequestForPoolScopeMetadata(handler http.Handler, multiplexerManager *multiplexer.MultiplexerManager, multiplexerUserAgent string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		if isMultiplexerRequest(req, multiplexerManager, multiplexerUserAgent) {
			ctx = util.WithIsRequestForPoolScopeMetadata(ctx, true)
		} else {
			ctx = util.WithIsRequestForPoolScopeMetadata(ctx, false)
		}
		req = req.WithContext(ctx)
		handler.ServeHTTP(w, req)
	})
}

func isMultiplexerRequest(req *http.Request, multiplexerManager *multiplexer.MultiplexerManager, multiplexerUserAgent string) bool {
	// the requests from multiplexer manager
	if req.UserAgent() == multiplexerUserAgent {
		return false
	}

	info, ok := apirequest.RequestInfoFrom(req.Context())
	if !ok {
		return false
	}

	// list/watch requests
	if info.Verb != "list" && info.Verb != "watch" {
		return false
	}

	return multiplexerManager.IsPoolScopeMetadata(&schema.GroupVersionResource{
		Group:    info.APIGroup,
		Version:  info.APIVersion,
		Resource: info.Resource,
	})
}

// WithPartialObjectMetadataRequest is used for extracting info for partial object metadata request,
// then these info is used by cache manager.
func WithPartialObjectMetadataRequest(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		if info, ok := apirequest.RequestInfoFrom(ctx); ok {
			if info.IsResourceRequest {
				var gvk schema.GroupVersionKind
				acceptHeader := req.Header.Get("Accept")
				clauses := goautoneg.ParseAccept(acceptHeader)
				if len(clauses) >= 1 {
					gvk.Group = clauses[0].Params["g"]
					gvk.Version = clauses[0].Params["v"]
					gvk.Kind = clauses[0].Params["as"]
				}

				if gvk.Kind == "PartialObjectMetadataList" || gvk.Kind == "PartialObjectMetadata" {
					if err := ensureValidGroupAndVersion(&gvk); err != nil {
						klog.Errorf("WithPartialObjectMetadataRequest error: %v", err)
					} else {
						ctx = util.WithConvertGVK(ctx, &gvk)
						req = req.WithContext(ctx)
					}
				}
			}
		}

		handler.ServeHTTP(w, req)
	})
}

func ensureValidGroupAndVersion(gvk *schema.GroupVersionKind) error {
	if strings.Contains(gvk.Group, "meta.k8s.io") {
		gvk.Group = "meta.k8s.io"
	} else {
		return fmt.Errorf("unknown group(%s) for partialobjectmetadata request", gvk.Group)
	}

	switch {
	case strings.Contains(gvk.Version, "v1"):
		gvk.Version = "v1"
	case strings.Contains(gvk.Version, "v1beta1"):
		gvk.Version = "v1beta1"
	default:
		return fmt.Errorf("unknown version(%s) for partialobjectmetadata request", gvk.Version)
	}

	return nil
}

// WithRequestContentType add req-content-type in request context.
// if no Accept header is set, the request will be reject with a message.
func WithRequestContentType(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		if info, ok := apirequest.RequestInfoFrom(ctx); ok {
			if info.IsResourceRequest {
				var contentType string
				header := req.Header.Get("Accept")
				parts := strings.Split(header, ",")
				if len(parts) >= 1 {
					contentType = parts[0]
				}

				subParts := strings.Split(contentType, ";")
				for i := range subParts {
					if strings.Contains(subParts[i], "as=") {
						contentType = subParts[0]
						break
					}
				}

				if len(contentType) != 0 {
					ctx = util.WithReqContentType(ctx, contentType)
					req = req.WithContext(ctx)
				}
			}
		}

		handler.ServeHTTP(w, req)
	})
}

// WithCacheHeaderCheck add cache agent for response cache
// in default mode, only kubelet, kube-proxy, flanneld, coredns User-Agent
// can be supported to cache response. and with Edge-Cache header is also supported.
func WithCacheHeaderCheck(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		if info, ok := apirequest.RequestInfoFrom(ctx); ok {
			if info.IsResourceRequest {
				needToCache := strings.ToLower(req.Header.Get(canCacheHeader))
				if needToCache == "true" {
					ctx = util.WithReqCanCache(ctx, true)
					req = req.WithContext(ctx)
				}
				req.Header.Del(canCacheHeader)
			}
		}

		handler.ServeHTTP(w, req)
	})
}

// selectorString returns the string of label and field selector
func selectorString(lSelector labels.Selector, fSelector fields.Selector) string {
	var ls string
	var fs string
	if lSelector != nil {
		ls = lSelector.String()
	}

	if fSelector != nil {
		fs = fSelector.String()
	}

	switch {
	case ls != "" && fs != "":
		return strings.Join([]string{ls, fs}, "&")

	case ls != "":
		return ls

	case fs != "":
		return fs
	}

	return ""
}

// WithListRequestSelector add label selector and field selector string in list request context.
func WithListRequestSelector(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		if info, ok := apirequest.RequestInfoFrom(ctx); ok {
			if info.IsResourceRequest && info.Verb == "list" && info.Name == "" {
				// list request with fieldSelector=metadata.name does not need to set selector string
				opts := metainternalversion.ListOptions{}
				if err := metainternalversionscheme.ParameterCodec.DecodeParameters(req.URL.Query(), metav1.SchemeGroupVersion, &opts); err == nil {
					if str := selectorString(opts.LabelSelector, opts.FieldSelector); str != "" {
						ctx = util.WithListSelector(ctx, str)
						req = req.WithContext(ctx)
					}
				}
			}
		}

		handler.ServeHTTP(w, req)
	})
}

// WithRequestClientComponent add component field in request context.
// component is extracted from User-Agent Header, and only the content
// before the "/" when User-Agent include "/".
func WithRequestClientComponent(handler http.Handler, mode util.WorkingMode) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		if info, ok := apirequest.RequestInfoFrom(ctx); ok {

			if info.IsResourceRequest {
				userAgent := strings.ToLower(req.Header.Get("User-Agent"))
				// on edge mode, component name is used as element of cache path, so only the part
				// before the first forward slash can be recognized as component name.
				if mode == util.WorkingModeEdge {
					parts := strings.Split(userAgent, "/")
					if len(parts) > 0 {
						userAgent = strings.ToLower(parts[0])
					}
				}

				if userAgent != "" {
					convertGVK, ok := util.ConvertGVKFrom(ctx)
					if ok && convertGVK != nil {
						userAgent = filepath.Join(userAgent, strings.Join([]string{"partialobjectmetadatas", convertGVK.Version, convertGVK.Group}, "."))
					}

					ctx = util.WithClientComponent(ctx, userAgent)
					req = req.WithContext(ctx)
				}
			}
		}

		handler.ServeHTTP(w, req)
	})
}

type wrapperResponseWriter struct {
	http.ResponseWriter
	http.Flusher
	http.Hijacker
	statusCode int
}

func newWrapperResponseWriter(w http.ResponseWriter) *wrapperResponseWriter {
	flusher, ok := w.(http.Flusher)
	if !ok {
		klog.Error("can not get http.Flusher")
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		klog.Error("can not get http.Hijacker")
	}

	return &wrapperResponseWriter{
		ResponseWriter: w,
		Flusher:        flusher,
		Hijacker:       hijacker,
	}
}

func (wrw *wrapperResponseWriter) WriteHeader(statusCode int) {
	wrw.statusCode = statusCode
	wrw.ResponseWriter.WriteHeader(statusCode)
}

// WithRequestTrace used to trace
// status code and
// latency for outward requests redirected from proxyserver to apiserver
func WithRequestTrace(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		info, ok := apirequest.RequestInfoFrom(ctx)
		client, _ := util.ClientComponentFrom(ctx)
		isRequestForPoolScopeMetadata, _ := util.IsRequestForPoolScopeMetadataFrom(ctx)
		if ok {
			if info.IsResourceRequest {
				if isRequestForPoolScopeMetadata {
					metrics.Metrics.IncInFlightMultiplexerRequests(info.Verb, info.Resource, info.Subresource, client)
					defer metrics.Metrics.DecInFlightMultiplexerRequests(info.Verb, info.Resource, info.Subresource, client)
				} else {
					metrics.Metrics.IncInFlightRequests(info.Verb, info.Resource, info.Subresource, client)
					defer metrics.Metrics.DecInFlightRequests(info.Verb, info.Resource, info.Subresource, client)
				}
			}
		} else {
			info = &apirequest.RequestInfo{}
		}
		wrapperRW := newWrapperResponseWriter(w)

		start := time.Now()
		defer func() {
			duration := time.Since(start)
			if info.Resource == "leases" {
				klog.V(5).Infof("%s with status code %d, spent %v", util.ReqString(req), wrapperRW.statusCode, duration)
			} else {
				klog.V(2).Infof("%s with status code %d, spent %v", util.ReqString(req), wrapperRW.statusCode, duration)
			}
			// 'watch' & 'proxy' requests don't need to be monitored in metrics
			if info.Verb != "proxy" && info.Verb != "watch" {
				metrics.Metrics.SetProxyLatencyCollector(client, info.Verb, info.Resource, info.Subresource, metrics.Apiserver_latency, int64(duration))
			}
		}()
		handler.ServeHTTP(wrapperRW, req)
	})
}

// WithRequestTraceFull used to trace the entire duration: coming to yurthub -> yurthub to apiserver -> leaving yurthub
func WithRequestTraceFull(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		info, ok := apirequest.RequestInfoFrom(req.Context())
		if !ok {
			info = &apirequest.RequestInfo{}
		}
		client, _ := util.ClientComponentFrom(req.Context())
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			// 'watch' & 'proxy' requests don't need to be monitored in metrics
			if info.Verb != "proxy" && info.Verb != "watch" {
				metrics.Metrics.SetProxyLatencyCollector(client, info.Verb, info.Resource, info.Subresource, metrics.Full_lantency, int64(duration))
			}
		}()
		handler.ServeHTTP(w, req)
	})
}

// WithMaxInFlightLimit limits the number of in-flight requests. and when in flight
// requests exceeds the threshold, the following incoming requests will be rejected.
func WithMaxInFlightLimit(handler http.Handler, limit int, nodeName string) http.Handler {
	var reqChan, multiplexerReqChan chan bool
	if limit > 0 {
		reqChan = make(chan bool, limit)
	}
	multiplexerReqChan = make(chan bool, 4096)

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		info, ok := apirequest.RequestInfoFrom(ctx)
		if !ok {
			info = &apirequest.RequestInfo{}
		}
		isRequestForPoolScopeMetadata, _ := util.IsRequestForPoolScopeMetadataFrom(ctx)
		if isRequestForPoolScopeMetadata {
			select {
			case multiplexerReqChan <- true:
				klog.V(2).Infof("%s, in flight requests for pool scope metadata: %d", util.ReqString(req), len(multiplexerReqChan))

				defer func() {
					<-multiplexerReqChan
					klog.V(5).Infof("%s request completed, left %d requests for pool scope metadata in flight", util.ReqString(req), len(multiplexerReqChan))
				}()
				handler.ServeHTTP(w, req)
			default:
				// Return a 429 status indicating "Too Many Requests"
				klog.Errorf("Too many requests for pool scope metadata, please try again later, %s", util.ReqString(req))
				metrics.Metrics.IncRejectedMultiplexerRequestCounter()
				w.Header().Set("Retry-After", "1")
				util.Err(errors.NewTooManyRequestsError(fmt.Sprintf("Too many multiplexer requests for node(%s), please try again later.", nodeName)), w, req)
			}
		} else {
			select {
			case reqChan <- true:
				if info.Resource == "leases" {
					klog.V(5).Infof("%s, in flight requests: %d", util.ReqString(req), len(reqChan))
				} else {
					klog.V(2).Infof("%s, in flight requests: %d", util.ReqString(req), len(reqChan))
				}
				defer func() {
					<-reqChan
					klog.V(5).Infof("%s request completed, left %d requests in flight", util.ReqString(req), len(reqChan))
				}()
				handler.ServeHTTP(w, req)
			default:
				// Return a 429 status indicating "Too Many Requests"
				klog.Errorf("Too many requests, please try again later, %s", util.ReqString(req))
				metrics.Metrics.IncRejectedRequestCounter()
				w.Header().Set("Retry-After", "1")
				util.Err(errors.NewTooManyRequestsError("Too many requests, please try again later."), w, req)
			}
		}
	})
}

// 1. WithRequestTimeout add timeout context for watch request.
//    timeout is TimeoutSeconds plus a margin(15 seconds). the timeout
//    context is used to cancel the request for hub missed disconnect
//    signal from kube-apiserver when watch request is ended.
// 2. WithRequestTimeout reduce timeout context for get/list request.
//    timeout is Timeout reduce a margin(2 seconds). When request remote server fail,
//    can get data from cache before client timeout.

func WithRequestTimeout(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if info, ok := apirequest.RequestInfoFrom(req.Context()); ok {
			if info.IsResourceRequest && needModifyTimeoutVerb[info.Verb] {
				var timeout time.Duration
				if info.Verb == "list" || info.Verb == "watch" {
					opts := metainternalversion.ListOptions{}
					if err := metainternalversionscheme.ParameterCodec.DecodeParameters(req.URL.Query(), metav1.SchemeGroupVersion, &opts); err != nil {
						klog.Errorf("could not decode parameter for list/watch request: %s", util.ReqString(req))
						util.Err(errors.NewBadRequest(err.Error()), w, req)
						return
					}
					if opts.TimeoutSeconds != nil {
						if info.Verb == "watch" {
							timeout = time.Duration(*opts.TimeoutSeconds+watchTimeoutMargin) * time.Second
						} else if *opts.TimeoutSeconds > getAndListTimeoutReduce {
							timeout = time.Duration(*opts.TimeoutSeconds-getAndListTimeoutReduce) * time.Second
						}
					}
				} else if info.Verb == "get" {
					query := req.URL.Query()
					if str := query["timeout"]; len(str) > 0 {
						if t, err := time.ParseDuration(str[0]); err == nil {
							if t > time.Duration(getAndListTimeoutReduce)*time.Second {
								timeout = t - time.Duration(getAndListTimeoutReduce)*time.Second
							}
						}
					}
				}
				if timeout > 0 {
					ctx, cancel := context.WithTimeout(req.Context(), timeout)
					defer cancel()
					req = req.WithContext(ctx)
				}
			}
		}
		handler.ServeHTTP(w, req)
	})
}

func WithSaTokenSubstitute(handler http.Handler, tenantMgr tenant.Interface) http.Handler {

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {

		if oldToken := util.ParseBearerToken(req.Header.Get("Authorization")); oldToken != "" { // bearer token is not empty&valid

			if jsonWebToken, err := jwt.ParseSigned(oldToken); err != nil {

				klog.Errorf("invalid bearer token %s, err: %v", oldToken, err)
			} else {
				oldClaim := jwt.Claims{}

				if err := jsonWebToken.UnsafeClaimsWithoutVerification(&oldClaim); err == nil {

					if tenantNs, _, err := serviceaccount.SplitUsername(oldClaim.Subject); err == nil {

						if tenantMgr.GetTenantNs() != tenantNs && tenantNs == "kube-system" && tenantMgr.WaitForCacheSync() { // token is not from tenant's namespace
							req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", tenantMgr.GetTenantToken()))
							klog.V(2).Infof("replace token, old: %s, new: %s", oldToken, tenantMgr.GetTenantToken())
						}

					} else {
						klog.Errorf("could not parse tenant ns from token, token %s, sub: %s", oldToken, oldClaim.Subject)
					}
				}
			}

		}

		handler.ServeHTTP(w, req)
	})
}

// IsListRequestWithNameFieldSelector will check if the request has FieldSelector "metadata.name".
// If found, return true, otherwise false.
func IsListRequestWithNameFieldSelector(req *http.Request) bool {
	ctx := req.Context()
	if info, ok := apirequest.RequestInfoFrom(ctx); ok {
		if info.IsResourceRequest && info.Verb == "list" {
			opts := metainternalversion.ListOptions{}
			if err := metainternalversionscheme.ParameterCodec.DecodeParameters(req.URL.Query(), metav1.SchemeGroupVersion, &opts); err == nil {
				if opts.FieldSelector == nil {
					return false
				}
				if _, found := opts.FieldSelector.RequiresExactMatch("metadata.name"); found {
					return true
				}
			}
		}
	}
	return false
}

// IsKubeletLeaseReq judge whether the request is a lease request from kubelet
func IsKubeletLeaseReq(req *http.Request) bool {
	ctx := req.Context()
	if comp, ok := util.ClientComponentFrom(ctx); !ok || comp != "kubelet" {
		return false
	}
	if info, ok := apirequest.RequestInfoFrom(ctx); !ok || info.Resource != "leases" {
		return false
	}
	return true
}

// IsKubeletGetNodeReq judge whether the request is a get node request from kubelet
func IsKubeletGetNodeReq(req *http.Request) bool {
	ctx := req.Context()
	if comp, ok := util.ClientComponentFrom(ctx); !ok || comp != "kubelet" {
		return false
	}
	if info, ok := apirequest.RequestInfoFrom(ctx); !ok || info.Resource != "nodes" || info.Verb != "get" {
		return false
	}
	return true
}

func IsSubjectAccessReviewCreateGetRequest(req *http.Request) bool {
	ctx := req.Context()
	info, ok := apirequest.RequestInfoFrom(ctx)
	if !ok {
		return false
	}

	comp, ok := util.ClientComponentFrom(ctx)
	if !ok {
		return false
	}

	return info.IsResourceRequest &&
		comp == "kubelet" &&
		info.Resource == "subjectaccessreviews" &&
		(info.Verb == "create" || info.Verb == "get")
}

func IsEventCreateRequest(req *http.Request) bool {
	ctx := req.Context()
	info, ok := apirequest.RequestInfoFrom(ctx)
	if !ok {
		return false
	}

	return info.IsResourceRequest &&
		info.Resource == "events" &&
		info.Verb == "create"
}

func ReListWatchReq(rw http.ResponseWriter, req *http.Request) {
	agent, _ := util.ClientComponentFrom(req.Context())
	klog.Infof("component %s request urL %s with rv = %s is rejected, expect re-list",
		agent, util.ReqString(req), req.URL.Query().Get("resourceVersion"))

	serializerManager := serializer.NewSerializerManager()
	mediaType, params, _ := mime.ParseMediaType(runtime.ContentTypeProtobuf)

	_, streamingSerializer, framer, err := serializerManager.WatchEventClientNegotiator.StreamDecoder(mediaType, params)
	if err != nil {
		klog.Errorf("ReListWatchReq %s failed with error = %s", util.ReqString(req), err.Error())
		return
	}

	streamingEncoder := streaming.NewEncoder(framer.NewFrameWriter(rw), streamingSerializer)

	outEvent := &metav1.WatchEvent{
		Type: string(watch.Error),
	}

	if err := streamingEncoder.Encode(outEvent); err != nil {
		klog.Errorf("ReListWatchReq %s failed with error = %s", util.ReqString(req), err.Error())
		return
	}

	klog.Infof("this request write error event back finished.")
	rw.(http.Flusher).Flush()
}
