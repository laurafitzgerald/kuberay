package ray

import (
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

var (
	testNetworkPolicyController  *NetworkPolicyController
	testRayClusterBasic          *rayv1.RayCluster
	testRayClusterWithRayJob     *rayv1.RayCluster
	testRayClusterWithOtherOwner *rayv1.RayCluster
)

func setupNetworkPolicyTest(_ *testing.T) {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	// Initialize NetworkPolicy controller
	testNetworkPolicyController = &NetworkPolicyController{
		Scheme: runtime.NewScheme(),
	}

	// Basic RayCluster without owner
	testRayClusterBasic = &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "ray-head",
								Image: "rayproject/ray:latest",
							},
						},
					},
				},
			},
		},
	}

	// RayCluster owned by RayJob
	testRayClusterWithRayJob = &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-rayjob",
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "ray.io/v1",
					Kind:       "RayJob",
					Name:       "test-job",
					UID:        "12345",
				},
			},
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "ray-head",
								Image: "rayproject/ray:latest",
							},
						},
					},
				},
			},
		},
	}

	// RayCluster owned by something other than RayJob
	testRayClusterWithOtherOwner = &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-other",
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-deployment",
					UID:        "67890",
				},
			},
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "ray-head",
								Image: "rayproject/ray:latest",
							},
						},
					},
				},
			},
		},
	}
}

func TestBuildNetworkPolicy_BasicCluster(t *testing.T) {
	setupNetworkPolicyTest(t)

	// Set environment for testing
	originalEnv := os.Getenv("POD_NAMESPACE")
	os.Setenv("POD_NAMESPACE", "ray-system")
	defer os.Setenv("POD_NAMESPACE", originalEnv)

	// Test building NetworkPolicy for basic cluster
	policy := testNetworkPolicyController.buildNetworkPolicy(testRayClusterBasic)

	// Verify basic properties
	expectedName := testRayClusterBasic.Name + "-default-deny"
	assert.Equal(t, expectedName, policy.Name)
	assert.Equal(t, testRayClusterBasic.Namespace, policy.Namespace)

	// Verify labels
	expectedLabels := map[string]string{
		utils.RayClusterLabelKey:                testRayClusterBasic.Name,
		utils.KubernetesApplicationNameLabelKey: utils.ApplicationName,
		utils.KubernetesCreatedByLabelKey:       utils.ComponentName,
	}
	assert.Equal(t, expectedLabels, policy.Labels)

	// Verify policy type
	assert.Equal(t, []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}, policy.Spec.PolicyTypes)

	// Verify pod selector
	expectedPodSelector := metav1.LabelSelector{
		MatchLabels: map[string]string{
			utils.RayClusterLabelKey: testRayClusterBasic.Name,
		},
	}
	assert.Equal(t, expectedPodSelector, policy.Spec.PodSelector)

	// Verify ingress rules - should have 2 peers (intra-cluster + operator)
	require.Len(t, policy.Spec.Ingress, 1)
	require.Len(t, policy.Spec.Ingress[0].From, 2)

	// Verify intra-cluster peer
	intraClusterPeer := policy.Spec.Ingress[0].From[0]
	expectedIntraClusterPeer := networkingv1.NetworkPolicyPeer{
		PodSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{
				utils.RayClusterLabelKey: testRayClusterBasic.Name,
			},
		},
	}
	assert.Equal(t, expectedIntraClusterPeer, intraClusterPeer)

	// Verify operator peer
	operatorPeer := policy.Spec.Ingress[0].From[1]
	expectedOperatorPeer := networkingv1.NetworkPolicyPeer{
		PodSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{
				utils.KubernetesApplicationNameLabelKey: utils.ApplicationName,
			},
		},
		NamespaceSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{
				"kubernetes.io/metadata.name": "ray-system",
			},
		},
	}
	assert.Equal(t, expectedOperatorPeer, operatorPeer)
}

func TestBuildNetworkPolicy_ClusterWithRayJob(t *testing.T) {
	setupNetworkPolicyTest(t)

	// Set environment for testing
	originalEnv := os.Getenv("POD_NAMESPACE")
	os.Setenv("POD_NAMESPACE", "ray-system")
	defer os.Setenv("POD_NAMESPACE", originalEnv)

	// Test building NetworkPolicy for cluster owned by RayJob
	policy := testNetworkPolicyController.buildNetworkPolicy(testRayClusterWithRayJob)

	// Verify basic properties
	expectedName := testRayClusterWithRayJob.Name + "-default-deny"
	assert.Equal(t, expectedName, policy.Name)

	// Verify ingress rules - should have 3 peers (intra-cluster + operator + rayjob)
	require.Len(t, policy.Spec.Ingress, 1)
	require.Len(t, policy.Spec.Ingress[0].From, 3)

	// Verify RayJob peer (should be the third peer)
	rayJobPeer := policy.Spec.Ingress[0].From[2]
	expectedRayJobPeer := networkingv1.NetworkPolicyPeer{
		PodSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{
				"batch.kubernetes.io/job-name": "test-job",
			},
		},
	}
	assert.Equal(t, expectedRayJobPeer, rayJobPeer)
}

func TestBuildNetworkPolicy_EnvironmentFallback(t *testing.T) {
	setupNetworkPolicyTest(t)

	// Test fallback when POD_NAMESPACE is not set
	originalEnv := os.Getenv("POD_NAMESPACE")
	os.Unsetenv("POD_NAMESPACE")
	defer os.Setenv("POD_NAMESPACE", originalEnv)

	policy := testNetworkPolicyController.buildNetworkPolicy(testRayClusterBasic)

	// Verify operator peer uses fallback namespace
	operatorPeer := policy.Spec.Ingress[0].From[1]
	assert.Equal(t, map[string]string{"kubernetes.io/metadata.name": "ray-system"},
		operatorPeer.NamespaceSelector.MatchLabels)
}

func TestBuildRayJobPeer_NoOwner(t *testing.T) {
	setupNetworkPolicyTest(t)

	// Test RayCluster without owner
	peer := testNetworkPolicyController.buildRayJobPeer(testRayClusterBasic)
	assert.Nil(t, peer)
}

func TestBuildRayJobPeer_WithRayJobOwner(t *testing.T) {
	setupNetworkPolicyTest(t)

	// Test RayCluster with RayJob owner
	peer := testNetworkPolicyController.buildRayJobPeer(testRayClusterWithRayJob)
	require.NotNil(t, peer)

	expectedPeer := &networkingv1.NetworkPolicyPeer{
		PodSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{
				"batch.kubernetes.io/job-name": "test-job",
			},
		},
	}
	assert.Equal(t, expectedPeer, peer)
}

func TestBuildRayJobPeer_WithOtherOwner(t *testing.T) {
	setupNetworkPolicyTest(t)

	// Test RayCluster with non-RayJob owner
	peer := testNetworkPolicyController.buildRayJobPeer(testRayClusterWithOtherOwner)
	assert.Nil(t, peer)
}

func TestBuildRayJobPeer_MultipleOwners(t *testing.T) {
	setupNetworkPolicyTest(t)

	// Create RayCluster with multiple owners, including RayJob
	rayCluster := testRayClusterBasic.DeepCopy()
	rayCluster.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
			Name:       "test-deployment",
			UID:        "67890",
		},
		{
			APIVersion: "ray.io/v1",
			Kind:       "RayJob",
			Name:       "test-job",
			UID:        "12345",
		},
	}

	peer := testNetworkPolicyController.buildRayJobPeer(rayCluster)
	require.NotNil(t, peer)

	expectedPeer := &networkingv1.NetworkPolicyPeer{
		PodSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{
				"batch.kubernetes.io/job-name": "test-job",
			},
		},
	}
	assert.Equal(t, expectedPeer, peer)
}

func TestBuildNetworkPolicy_DifferentNamespace(t *testing.T) {
	setupNetworkPolicyTest(t)

	// Set custom operator namespace
	originalEnv := os.Getenv("POD_NAMESPACE")
	os.Setenv("POD_NAMESPACE", "custom-ray-system")
	defer os.Setenv("POD_NAMESPACE", originalEnv)

	// Create cluster in different namespace
	rayCluster := testRayClusterBasic.DeepCopy()
	rayCluster.Namespace = "custom-namespace"

	policy := testNetworkPolicyController.buildNetworkPolicy(rayCluster)

	// Verify NetworkPolicy is created in the same namespace as RayCluster
	assert.Equal(t, "custom-namespace", policy.Namespace)

	// Verify operator peer references custom namespace
	operatorPeer := policy.Spec.Ingress[0].From[1]
	assert.Equal(t, map[string]string{"kubernetes.io/metadata.name": "custom-ray-system"},
		operatorPeer.NamespaceSelector.MatchLabels)
}

func TestBuildNetworkPolicy_LongClusterName(t *testing.T) {
	setupNetworkPolicyTest(t)

	// Test with long cluster name
	longName := "very-long-cluster-name-that-might-cause-issues"
	rayCluster := testRayClusterBasic.DeepCopy()
	rayCluster.Name = longName

	policy := testNetworkPolicyController.buildNetworkPolicy(rayCluster)

	// Verify name is constructed correctly
	expectedName := longName + "-default-deny"
	assert.Equal(t, expectedName, policy.Name)

	// Verify pod selector uses correct cluster name
	expectedPodSelector := metav1.LabelSelector{
		MatchLabels: map[string]string{
			utils.RayClusterLabelKey: longName,
		},
	}
	assert.Equal(t, expectedPodSelector, policy.Spec.PodSelector)
}
