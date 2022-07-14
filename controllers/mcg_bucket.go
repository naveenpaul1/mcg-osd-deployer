/*
Copyright 2022.

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

package controllers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kube-object-storage/lib-bucket-provisioner/pkg/apis/objectbucket.io/v1alpha1"
	noobaav1alpha1 "github.com/noobaa/noobaa-operator/v5/pkg/apis/noobaa/v1alpha1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilwait "k8s.io/apimachinery/pkg/util/wait"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	McgmsObcNamespace   = "mcgms-obc-namespace"
	DefaultBackingStore = "noobaa-default-backing-store"
	READY               = "Ready"
	rhodsNamesapce      = "rhods-notebooks"
	rhodsSecretName     = "rhods-secret-name"
)

func (r *ManagedMCGReconciler) bucketClassAdded(object client.Object) {
	basebucketClass, _ := object.(*noobaav1alpha1.BucketClass)
	bucketName := basebucketClass.Name
	annotations := basebucketClass.Annotations
	if _, ok := annotations[McgmsObcNamespace]; ok && r.isBucketClassCreationSuccess(*basebucketClass) {
		if r.ctx == nil {
			r.ctx = context.Background()
		}
		if r.getObjectBucketClaim(object) != nil {
			r.Log.Info("OBC already exists", "name", bucketName)

			return
		}
		r.objectBucketClaim = &noobaav1alpha1.ObjectBucketClaim{}
		obc := r.setOBCDesiredState(bucketName, *basebucketClass)
		_, err := ctrl.CreateOrUpdate(r.ctx, r.Client, r.objectBucketClaim, func() error {
			r.Log.Info("Creating OBC ", "name", bucketName)
			r.objectBucketClaim.ObjectMeta.OwnerReferences = obc.ObjectMeta.OwnerReferences
			r.objectBucketClaim.Spec = obc.Spec

			return nil
		})
		r.shareSecretWithRhods(annotations, err)
	}
}

func (r *ManagedMCGReconciler) shareSecretWithRhods(annotations map[string]string, err error) bool {
	rhodsSecret := ""
	var ok bool
	if rhodsSecret, ok = annotations[rhodsSecretName]; !ok || rhodsSecret == "" {
		r.Log.Info("rhods secret name annotations missing")
		return true
	}
	rohdsSecret := GetSecret(rhodsSecret, rhodsNamesapce)
	err = r.UnrestrictedClient.Get(r.ctx, types.NamespacedName{
		Name:      rhodsSecret,
		Namespace: rhodsNamesapce,
	}, rohdsSecret)
	if err != nil {
		r.Log.Error(err, "rhods secret missing")
		return true
	}
	timeout := 10 * time.Second
	interval := 2 * time.Second
	secretName := r.objectBucketClaim.Name
	secretNamespace := r.objectBucketClaim.Namespace
	obcSecret := GetSecret(secretName, secretNamespace)
	err = utilwait.PollImmediate(interval, timeout, func() (done bool, err error) {
		erro := r.UnrestrictedClient.Get(r.ctx, types.NamespacedName{
			Name:      secretName,
			Namespace: secretNamespace,
		}, obcSecret)
		if erro != nil {
			return false, err
		}
		if obcSecret.UID == "" {
			return false, fmt.Errorf("Error reconciling ManagedMCG")
		}
		return true, nil
	})
	if err != nil {
		r.Log.Error(err, "Unable to get OBC Secret")
	}
	if len(obcSecret.Data) == 0 || rohdsSecret.UID == "" {
		r.Log.Info("OBC or RRHODS secret is missing")
		return true
	}
	for k, v := range obcSecret.Data {
		rohdsSecret.Data[k] = v
	}
	rohdsSecret.Data["BUCKET_HOST"] = []byte("s3." + r.namespace + ".svc")
	if err := r.UnrestrictedClient.Update(r.ctx, rohdsSecret); err != nil {
		r.Log.Error(err, "failed to update RHODS secret:")
	}
	return false
}

func GetSecret(name string, namespace string) *v1.Secret {
	return &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
}

func (r *ManagedMCGReconciler) getDefaultBackingStore() string {
	backingStores := noobaav1alpha1.BackingStoreList{}
	if err := r.list(&backingStores); err != nil {
		r.Log.Error(err, "error getting BackingStore list")

		return ""
	}
	if len(backingStores.Items) == 0 {
		return ""
	}
	for _, backingStore := range backingStores.Items {
		if strings.HasPrefix(backingStore.Name, DefaultBackingStore) {
			return DefaultBackingStore
		}
	}

	return backingStores.Items[0].Name
}

func (r *ManagedMCGReconciler) isBucketClassCreationSuccess(bucketClass noobaav1alpha1.BucketClass) bool {
	if bucketClass.Status.Phase == READY {
		return true
	}

	timeout := 10 * time.Second
	interval := 2 * time.Second
	err := utilwait.PollImmediate(interval, timeout, func() (done bool, err error) {
		r.bucketClass = &noobaav1alpha1.BucketClass{}
		r.bucketClass.Name = bucketClass.GetName()
		r.bucketClass.Namespace = bucketClass.GetNamespace()
		err = r.get(r.bucketClass)
		if err != nil {
			r.Log.Error(err, "Unable to get bucketClass")

			return false, nil
		}
		if r.bucketClass.Status.Phase != READY {
			return false, nil
		}

		return true, nil
	})
	if err != nil {
		r.Log.Error(err, "Unable to get bucketClass")

		return false
	}
	r.Log.Info("BucketClass creation was success", "name", r.bucketClass.Name)

	return true
}

func (r *ManagedMCGReconciler) getObjectBucketClaim(object client.Object) *v1alpha1.ObjectBucketClaim {
	objectBucketClaim := r.newObjectBucketClaim(object)
	err := r.get(&objectBucketClaim)
	if err != nil {
		r.Log.Error(err, "ObjectBucketClaim resource not found")

		return nil
	}

	return &objectBucketClaim
}

func (r *ManagedMCGReconciler) setOBCDesiredState(bucketName string,
	bucketClass noobaav1alpha1.BucketClass,
) v1alpha1.ObjectBucketClaim {
	AdditionalConfigMap := make(map[string]string)
	AdditionalConfigMap["bucketclass"] = bucketName
	obc := noobaav1alpha1.ObjectBucketClaim{
		Spec: noobaav1alpha1.ObjectBucketClaimSpec{
			BucketName:         bucketName,
			StorageClassName:   bucketClass.GetNamespace() + ".noobaa.io",
			GenerateBucketName: bucketName,
			AdditionalConfig:   AdditionalConfigMap,
		},
	}
	r.objectBucketClaim.Name = bucketName
	r.objectBucketClaim.Namespace = r.getOBCCreationNamespace(bucketClass)
	obc.ObjectMeta.OwnerReferences = bucketClass.OwnerReferences

	return obc
}

func (r *ManagedMCGReconciler) getOBCCreationNamespace(bucketClass noobaav1alpha1.BucketClass) string {
	if obcNamespace, ok := bucketClass.GetAnnotations()[McgmsObcNamespace]; ok {
		return obcNamespace
	}

	return bucketClass.GetNamespace()
}

func (r *ManagedMCGReconciler) bucketClassDeleted(object client.Object) {
	objectBucketClaim := r.newObjectBucketClaim(object)
	err := r.delete(&objectBucketClaim)
	if err != nil {
		r.Log.Error(err, "error deleting ObjectBucketClaim")
	}
	r.Log.Info("ObjectBucketClaim deleted", "name", objectBucketClaim.Name)
}

func (*ManagedMCGReconciler) newObjectBucketClaim(object client.Object) v1alpha1.ObjectBucketClaim {
	annotations := object.GetAnnotations()
	objectBucketClaim := noobaav1alpha1.ObjectBucketClaim{}
	objectBucketClaim.Name = object.GetName()
	objectBucketClaim.Namespace = annotations[McgmsObcNamespace]

	return objectBucketClaim
}
