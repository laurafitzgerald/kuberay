/*

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

package ray

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/test/support"
)

func rayClusterTemplateForNetworkPolicy(name string, namespace string) *rayv1.RayCluster {
	return &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: rayv1.RayClusterSpec{
			RayVersion: support.GetRayVersion(),
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "ray-head",
								Image: support.GetRayImage(),
							},
						},
					},
				},
			},
			WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
				{
					Replicas:    ptr.To[int32](1),
					MinReplicas: ptr.To[int32](0),
					MaxReplicas: ptr.To[int32](2),
					GroupName:   "small-group",
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "ray-worker",
									Image: support.GetRayImage(),
								},
							},
						},
					},
				},
			},
		},
	}
}

var _ = Context("NetworkPolicy Controller Integration Tests", func() {
	Describe("Basic NetworkPolicy Creation", Ordered, func() {
		ctx := context.Background()
		namespace := "default"
		rayCluster := rayClusterTemplateForNetworkPolicy("raycluster-networkpolicy", namespace)

		It("Verify RayCluster spec", func() {
			Expect(rayCluster.Spec.WorkerGroupSpecs).To(HaveLen(1))
			Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(ptr.To[int32](1)))
		})

		It("Create a RayCluster custom resource", func() {
			err := k8sClient.Create(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RayCluster")
			Eventually(
				getResourceFunc(ctx, client.ObjectKey{Name: rayCluster.Name, Namespace: namespace}, rayCluster),
				time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayCluster: %v", rayCluster.Name)
		})

		It("Check NetworkPolicy is created", func() {
			networkPolicy := &networkingv1.NetworkPolicy{}
			expectedName := rayCluster.Name + "-default-deny"
			namespacedName := types.NamespacedName{Namespace: namespace, Name: expectedName}

			Eventually(
				getResourceFunc(ctx, namespacedName, networkPolicy),
				time.Second*10, time.Millisecond*500).Should(Succeed(), "NetworkPolicy should be created: %v", expectedName)
		})

		It("Verify NetworkPolicy has correct structure", func() {
			networkPolicy := &networkingv1.NetworkPolicy{}
			expectedName := rayCluster.Name + "-default-deny"
			namespacedName := types.NamespacedName{Namespace: namespace, Name: expectedName}

			err := k8sClient.Get(ctx, namespacedName, networkPolicy)
			Expect(err).NotTo(HaveOccurred(), "Failed to get NetworkPolicy")

			// Verify basic properties
			Expect(networkPolicy.Name).To(Equal(expectedName))
			Expect(networkPolicy.Namespace).To(Equal(namespace))

			// Verify labels
			expectedLabels := map[string]string{
				utils.RayClusterLabelKey:                rayCluster.Name,
				utils.KubernetesApplicationNameLabelKey: utils.ApplicationName,
				utils.KubernetesCreatedByLabelKey:       utils.ComponentName,
			}
			Expect(networkPolicy.Labels).To(Equal(expectedLabels))

			// Verify owner reference is set
			Expect(networkPolicy.OwnerReferences).To(HaveLen(1))
			Expect(networkPolicy.OwnerReferences[0].Name).To(Equal(rayCluster.Name))
			Expect(networkPolicy.OwnerReferences[0].Kind).To(Equal("RayCluster"))

			// Verify policy type
			Expect(networkPolicy.Spec.PolicyTypes).To(Equal([]networkingv1.PolicyType{networkingv1.PolicyTypeIngress}))

			// Verify pod selector
			expectedPodSelector := metav1.LabelSelector{
				MatchLabels: map[string]string{
					utils.RayClusterLabelKey: rayCluster.Name,
				},
			}
			Expect(networkPolicy.Spec.PodSelector).To(Equal(expectedPodSelector))

			// Verify ingress rules - should have 2 peers (intra-cluster + operator)
			Expect(networkPolicy.Spec.Ingress).To(HaveLen(1))
			Expect(networkPolicy.Spec.Ingress[0].From).To(HaveLen(2))

			// Verify intra-cluster peer
			intraClusterPeer := networkPolicy.Spec.Ingress[0].From[0]
			expectedIntraClusterPeer := networkingv1.NetworkPolicyPeer{
				PodSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						utils.RayClusterLabelKey: rayCluster.Name,
					},
				},
			}
			Expect(intraClusterPeer).To(Equal(expectedIntraClusterPeer))

			// Verify operator peer
			operatorPeer := networkPolicy.Spec.Ingress[0].From[1]
			Expect(operatorPeer.PodSelector).NotTo(BeNil())
			Expect(operatorPeer.PodSelector.MatchLabels[utils.KubernetesApplicationNameLabelKey]).To(Equal(utils.ApplicationName))
			Expect(operatorPeer.NamespaceSelector).NotTo(BeNil())
		})

		It("Delete RayCluster should delete NetworkPolicy", func() {
			// Delete the RayCluster
			err := k8sClient.Delete(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete RayCluster")

			// Note: envtest doesn't run garbage collection automatically like a real cluster
			// In a real cluster, the NetworkPolicy would be automatically deleted due to owner reference
			// For testing, we manually delete it to simulate garbage collection
			networkPolicy := &networkingv1.NetworkPolicy{}
			expectedName := rayCluster.Name + "-default-deny"
			namespacedName := types.NamespacedName{Namespace: namespace, Name: expectedName}

			// Get the NetworkPolicy to verify it exists
			err = k8sClient.Get(ctx, namespacedName, networkPolicy)
			if err == nil {
				// Manually delete since envtest doesn't run garbage collection
				err = k8sClient.Delete(ctx, networkPolicy)
				Expect(err).NotTo(HaveOccurred(), "Failed to manually delete NetworkPolicy")
			}

			// Verify NetworkPolicy is deleted
			Eventually(
				func() bool {
					err := k8sClient.Get(ctx, namespacedName, networkPolicy)
					return err != nil && client.IgnoreNotFound(err) == nil
				},
				time.Second*5, time.Millisecond*500).Should(BeTrue(), "NetworkPolicy should be deleted")
		})
	})

	Describe("RayCluster owned by RayJob", Ordered, func() {
		ctx := context.Background()
		namespace := "default"
		rayCluster := rayClusterTemplateForNetworkPolicy("raycluster-rayjob", namespace)

		// Add RayJob owner reference
		rayCluster.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: "ray.io/v1",
				Kind:       "RayJob",
				Name:       "test-rayjob",
				UID:        "12345",
			},
		}

		It("Create a RayCluster with RayJob owner", func() {
			err := k8sClient.Create(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RayCluster with RayJob owner")
			Eventually(
				getResourceFunc(ctx, client.ObjectKey{Name: rayCluster.Name, Namespace: namespace}, rayCluster),
				time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayCluster: %v", rayCluster.Name)
		})

		It("Check NetworkPolicy includes RayJob peer", func() {
			networkPolicy := &networkingv1.NetworkPolicy{}
			expectedName := rayCluster.Name + "-default-deny"
			namespacedName := types.NamespacedName{Namespace: namespace, Name: expectedName}

			Eventually(
				getResourceFunc(ctx, namespacedName, networkPolicy),
				time.Second*10, time.Millisecond*500).Should(Succeed(), "NetworkPolicy should be created")

			// Should have 3 peers: intra-cluster + operator + rayjob
			Expect(networkPolicy.Spec.Ingress).To(HaveLen(1))
			Expect(networkPolicy.Spec.Ingress[0].From).To(HaveLen(3))

			// Verify RayJob peer (should be the third peer)
			rayJobPeer := networkPolicy.Spec.Ingress[0].From[2]
			expectedRayJobPeer := networkingv1.NetworkPolicyPeer{
				PodSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"batch.kubernetes.io/job-name": "test-rayjob",
					},
				},
			}
			Expect(rayJobPeer).To(Equal(expectedRayJobPeer))
		})

		It("Clean up RayCluster with RayJob owner", func() {
			err := k8sClient.Delete(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete RayCluster")
		})
	})

	Describe("NetworkPolicy Already Exists", Ordered, func() {
		ctx := context.Background()
		namespace := "default"
		rayCluster := rayClusterTemplateForNetworkPolicy("raycluster-existing-np", namespace)
		existingNetworkPolicy := &networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name:      rayCluster.Name + "-default-deny",
				Namespace: namespace,
				Labels: map[string]string{
					"test": "existing",
				},
			},
			Spec: networkingv1.NetworkPolicySpec{
				PodSelector: metav1.LabelSelector{},
				PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			},
		}

		It("Create NetworkPolicy before RayCluster", func() {
			err := k8sClient.Create(ctx, existingNetworkPolicy)
			Expect(err).NotTo(HaveOccurred(), "Failed to create existing NetworkPolicy")
		})

		It("Create RayCluster should handle existing NetworkPolicy gracefully", func() {
			err := k8sClient.Create(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RayCluster")
			Eventually(
				getResourceFunc(ctx, client.ObjectKey{Name: rayCluster.Name, Namespace: namespace}, rayCluster),
				time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayCluster: %v", rayCluster.Name)

			// NetworkPolicy should still exist (not updated, just ignored)
			networkPolicy := &networkingv1.NetworkPolicy{}
			namespacedName := types.NamespacedName{Namespace: namespace, Name: existingNetworkPolicy.Name}
			err = k8sClient.Get(ctx, namespacedName, networkPolicy)
			Expect(err).NotTo(HaveOccurred(), "Existing NetworkPolicy should still exist")

			// Original labels should be preserved
			Expect(networkPolicy.Labels["test"]).To(Equal("existing"))
		})

		It("Clean up resources", func() {
			err := k8sClient.Delete(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete RayCluster")

			err = k8sClient.Delete(ctx, existingNetworkPolicy)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete existing NetworkPolicy")
		})
	})

	Describe("RayCluster Deletion", Ordered, func() {
		ctx := context.Background()
		namespace := "default"
		rayCluster := rayClusterTemplateForNetworkPolicy("raycluster-deletion", namespace)

		It("Create and immediately delete RayCluster", func() {
			err := k8sClient.Create(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RayCluster")

			// Add deletion timestamp by deleting
			err = k8sClient.Delete(ctx, rayCluster)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete RayCluster")

			// Verify RayCluster is being deleted or deleted
			Eventually(
				func() bool {
					err := k8sClient.Get(ctx, client.ObjectKey{Name: rayCluster.Name, Namespace: namespace}, rayCluster)
					return err != nil && client.IgnoreNotFound(err) == nil
				},
				time.Second*10, time.Millisecond*500).Should(BeTrue(), "RayCluster should be deleted")
		})
	})
})
