/*
Copyright 2024 The OpenYurt Authors.

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

package podenvupdater

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/openyurtio/openyurt/pkg/util"
	"github.com/openyurtio/openyurt/pkg/yurthub/filter/base"
)

const (
	masterHost = "169.254.2.1"
)

func TestRegister(t *testing.T) {
	filters := base.NewFilters([]string{})
	Register(filters)
	if !filters.Enabled(FilterName) {
		t.Errorf("couldn't register %s filter", FilterName)
	}
}

func TestName(t *testing.T) {
	nif, _ := NewPodEnvFilter()
	if nif.Name() != FilterName {
		t.Errorf("expect %s, but got %s", FilterName, nif.Name())
	}
}

func TestSupportedResourceAndVerbs(t *testing.T) {
	nif, _ := NewPodEnvFilter()
	rvs := nif.SupportedResourceAndVerbs()
	if len(rvs) != 1 {
		t.Errorf("supported more than one resources, %v", rvs)
	}
	for resource, verbs := range rvs {
		if resource != "pods" {
			t.Errorf("expect resource is pods, but got %s", resource)
		}
		if !verbs.Equal(sets.New("list", "watch", "get", "patch")) {
			t.Errorf("expect verbs are list/watch, but got %v", verbs.UnsortedList())
		}
	}
}

func TestFilterPodEnv(t *testing.T) {
	tests := []struct {
		name        string
		requestObj  runtime.Object
		expectedObj runtime.Object
	}{
		{
			name: "KUBERNETES_SERVICE_HOST set to original value",
			requestObj: &corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "test-container",
							Env: []corev1.EnvVar{
								{Name: "KUBERNETES_SERVICE_HOST", Value: "original-value"},
								{Name: "OTHER_ENV_VAR", Value: "some-value"},
							},
						},
					},
				},
			},
			expectedObj: &corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "test-container",
							Env: []corev1.EnvVar{
								{Name: "KUBERNETES_SERVICE_HOST", Value: masterHost},
								{Name: "OTHER_ENV_VAR", Value: "some-value"},
							},
						},
					},
				},
			},
		},
		{
			name: "KUBERNETES_SERVICE_HOST set to correct value, should update nothing",
			requestObj: &corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "test-container",
							Env: []corev1.EnvVar{
								{Name: "KUBERNETES_SERVICE_HOST", Value: masterHost},
								{Name: "OTHER_ENV_VAR", Value: "some-value"},
							},
						},
					},
				},
			},
			expectedObj: &corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "test-container",
							Env: []corev1.EnvVar{
								{Name: "KUBERNETES_SERVICE_HOST", Value: masterHost},
								{Name: "OTHER_ENV_VAR", Value: "some-value"},
							},
						},
					},
				},
			},
		},
		{
			name: "KUBERNETES_SERVICE_HOST does not exist",
			requestObj: &corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "test-container",
							Env: []corev1.EnvVar{
								{Name: "OTHER_ENV_VAR", Value: "some-value"},
							},
						},
					},
				},
			},
			expectedObj: &corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "test-container",
							Env: []corev1.EnvVar{
								{Name: "OTHER_ENV_VAR", Value: "some-value"},
							},
						},
					},
				},
			},
		},
		{
			name: "KUBERNETES_SERVICE_HOST updates correctly in multiple containers",
			requestObj: &corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "test-container",
							Env: []corev1.EnvVar{
								{Name: "KUBERNETES_SERVICE_HOST", Value: "original-value"},
								{Name: "OTHER_ENV_VAR", Value: "some-value"},
							},
						},
						{
							Name: "test-container1",
							Env: []corev1.EnvVar{
								{Name: "KUBERNETES_SERVICE_HOST", Value: "original-value"},
								{Name: "OTHER_ENV_VAR", Value: "some-value"},
							},
						},
					},
				},
			},
			expectedObj: &corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "test-container",
							Env: []corev1.EnvVar{
								{Name: "KUBERNETES_SERVICE_HOST", Value: masterHost},
								{Name: "OTHER_ENV_VAR", Value: "some-value"},
							},
						},
						{
							Name: "test-container1",
							Env: []corev1.EnvVar{
								{Name: "KUBERNETES_SERVICE_HOST", Value: masterHost},
								{Name: "OTHER_ENV_VAR", Value: "some-value"},
							},
						},
					},
				},
			},
		},
	}
	stopCh := make(<-chan struct{})

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pef, _ := NewPodEnvFilter()
			pef.SetMasterServiceHost(masterHost)
			newObj := pef.Filter(tc.requestObj, stopCh)

			if tc.expectedObj == nil {
				if !util.IsNil(newObj) {
					t.Errorf("RuntimeObjectFilter expect nil obj, but got %v", newObj)
				}
			} else if !reflect.DeepEqual(newObj, tc.expectedObj) {
				t.Errorf("RuntimeObjectFilter got error, expected: \n%v\nbut got: \n%v\n", tc.expectedObj, newObj)
			}
		})
	}
}

func TestFilterMasterPortNoop(t *testing.T) {
	podReq := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "test-container",
					Env: []corev1.EnvVar{
						{Name: "KUBERNETES_SERVICE_HOST", Value: masterHost},
						{Name: "OTHER_ENV_VAR", Value: "some-value"},
					},
				},
			},
		},
	}
	pef, _ := NewPodEnvFilter()
	pef.SetMasterServicePort("10261")
	newObj := pef.Filter(podReq, make(<-chan struct{}))

	if !reflect.DeepEqual(newObj, podReq) {
		t.Errorf("RuntimeObjectFilter got error, expected: \n%v\nbut got: \n%v\n", podReq, newObj)
	}
}

func TestFilterNonPodEnv(t *testing.T) {
	serviceReq := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc1",
			Namespace: "default",
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "10.96.105.187",
			Type:      corev1.ServiceTypeClusterIP,
		},
	}
	pef, _ := NewPodEnvFilter()
	newObj := pef.Filter(serviceReq, make(<-chan struct{}))

	if !reflect.DeepEqual(newObj, serviceReq) {
		t.Errorf("RuntimeObjectFilter got error, expected: \n%v\nbut got: \n%v\n", serviceReq, newObj)
	}
}
