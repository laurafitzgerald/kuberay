package ray

import (
	"context"
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

// NetworkPolicyController is a completely independent controller that watches RayCluster
// resources and manages NetworkPolicies for them.
type NetworkPolicyController struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;delete;patch
// +kubebuilder:rbac:groups=ray.io,resources=rayclusters,verbs=get;list;watch

// NewNetworkPolicyController creates a new independent NetworkPolicy controller
func NewNetworkPolicyController(mgr manager.Manager) *NetworkPolicyController {
	return &NetworkPolicyController{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("networkpolicy-controller"),
	}
}

// Reconcile handles RayCluster resources and creates/manages NetworkPolicies
func (r *NetworkPolicyController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx).WithName("networkpolicy-controller")

	// Fetch the RayCluster instance
	instance := &rayv1.RayCluster{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if errors.IsNotFound(err) {
			// RayCluster was deleted - NetworkPolicies will be garbage collected automatically
			logger.Info("RayCluster not found, NetworkPolicies will be garbage collected")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Check if RayCluster is being deleted
	if instance.DeletionTimestamp != nil {
		logger.Info("RayCluster is being deleted, NetworkPolicies will be garbage collected")
		return ctrl.Result{}, nil
	}

	logger.Info("Reconciling NetworkPolicies for RayCluster", "cluster", instance.Name)

	// Get KubeRay operator namespaces
	kubeRayNamespaces := r.getKubeRayNamespaces(ctx)

	// Create or update head NetworkPolicy
	headNetworkPolicy := r.buildHeadNetworkPolicy(instance, kubeRayNamespaces)
	if err := r.createOrUpdateNetworkPolicy(ctx, instance, headNetworkPolicy); err != nil {
		return ctrl.Result{}, err
	}

	// Create or update worker NetworkPolicy
	workerNetworkPolicy := r.buildWorkerNetworkPolicy(instance)
	if err := r.createOrUpdateNetworkPolicy(ctx, instance, workerNetworkPolicy); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("Successfully reconciled NetworkPolicies for RayCluster", "cluster", instance.Name)
	return ctrl.Result{}, nil
}

// getKubeRayNamespaces returns the list of KubeRay operator namespaces
func (r *NetworkPolicyController) getKubeRayNamespaces(ctx context.Context) []string {
	operatorNamespace := os.Getenv("POD_NAMESPACE")
	if operatorNamespace == "" {
		operatorNamespace = "ray-system" // fallback
	}
	return []string{operatorNamespace}
}

// createOrUpdateNetworkPolicy creates or updates a NetworkPolicy
func (r *NetworkPolicyController) createOrUpdateNetworkPolicy(ctx context.Context, instance *rayv1.RayCluster, networkPolicy *networkingv1.NetworkPolicy) error {
	logger := ctrl.LoggerFrom(ctx).WithName("networkpolicy-controller")

	// Set owner reference for garbage collection
	if err := controllerutil.SetControllerReference(instance, networkPolicy, r.Scheme); err != nil {
		return err
	}

	// Try to create the NetworkPolicy
	if err := r.Create(ctx, networkPolicy); err != nil {
		if errors.IsAlreadyExists(err) {
			// NetworkPolicy exists, update it
			existing := &networkingv1.NetworkPolicy{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(networkPolicy), existing); err != nil {
				return err
			}

			// Update the existing NetworkPolicy
			existing.Spec = networkPolicy.Spec
			existing.Labels = networkPolicy.Labels

			if err := r.Update(ctx, existing); err != nil {
				r.Recorder.Eventf(instance, corev1.EventTypeWarning, string(utils.FailedToCreateNetworkPolicy),
					"Failed to update NetworkPolicy %s/%s: %v", networkPolicy.Namespace, networkPolicy.Name, err)
				return err
			}

			logger.Info("Successfully updated NetworkPolicy", "name", networkPolicy.Name)
			r.Recorder.Eventf(instance, corev1.EventTypeNormal, string(utils.CreatedNetworkPolicy),
				"Updated NetworkPolicy %s/%s", networkPolicy.Namespace, networkPolicy.Name)
		} else {
			r.Recorder.Eventf(instance, corev1.EventTypeWarning, string(utils.FailedToCreateNetworkPolicy),
				"Failed to create NetworkPolicy %s/%s: %v", networkPolicy.Namespace, networkPolicy.Name, err)
			return err
		}
	} else {
		logger.Info("Successfully created NetworkPolicy", "name", networkPolicy.Name)
		r.Recorder.Eventf(instance, corev1.EventTypeNormal, string(utils.CreatedNetworkPolicy),
			"Created NetworkPolicy %s/%s", networkPolicy.Namespace, networkPolicy.Name)
	}

	return nil
}

// buildHeadNetworkPolicy creates a NetworkPolicy for Ray head pods
func (r *NetworkPolicyController) buildHeadNetworkPolicy(instance *rayv1.RayCluster, kubeRayNamespaces []string) *networkingv1.NetworkPolicy {
	labels := map[string]string{
		utils.RayClusterLabelKey:                instance.Name,
		utils.KubernetesApplicationNameLabelKey: utils.ApplicationName,
		utils.KubernetesCreatedByLabelKey:       utils.ComponentName,
	}

	// Build secured ports - including mTLS port
	allSecuredPorts := []networkingv1.NetworkPolicyPort{
		{
			Protocol: &[]corev1.Protocol{corev1.ProtocolTCP}[0],
			Port:     &[]intstr.IntOrString{intstr.FromInt(8443)}[0],
		},
		{
			Protocol: &[]corev1.Protocol{corev1.ProtocolTCP}[0],
			Port:     &[]intstr.IntOrString{intstr.FromInt(10001)}[0],
		},
	}

	// Build ingress rules
	ingressRules := []networkingv1.NetworkPolicyIngressRule{
		// Allow intra-cluster communication
		{
			From: []networkingv1.NetworkPolicyPeer{
				{
					PodSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							utils.RayClusterLabelKey: instance.Name,
						},
					},
				},
			},
		},
		// Allow dashboard and GCS access from any pod (for external access)
		{
			Ports: []networkingv1.NetworkPolicyPort{
				{
					Protocol: &[]corev1.Protocol{corev1.ProtocolTCP}[0],
					Port:     &[]intstr.IntOrString{intstr.FromInt(10001)}[0],
				},
				{
					Protocol: &[]corev1.Protocol{corev1.ProtocolTCP}[0],
					Port:     &[]intstr.IntOrString{intstr.FromInt(8265)}[0],
				},
			},
			From: []networkingv1.NetworkPolicyPeer{
				{
					PodSelector: &metav1.LabelSelector{},
				},
			},
		},
		// Allow KubeRay operator access
		{
			From: []networkingv1.NetworkPolicyPeer{
				{
					PodSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							utils.KubernetesApplicationNameLabelKey: utils.ApplicationName,
						},
					},
					NamespaceSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      corev1.LabelMetadataName,
								Operator: metav1.LabelSelectorOpIn,
								Values:   kubeRayNamespaces,
							},
						},
					},
				},
			},
			Ports: []networkingv1.NetworkPolicyPort{
				{
					Protocol: &[]corev1.Protocol{corev1.ProtocolTCP}[0],
					Port:     &[]intstr.IntOrString{intstr.FromInt(8265)}[0],
				},
				{
					Protocol: &[]corev1.Protocol{corev1.ProtocolTCP}[0],
					Port:     &[]intstr.IntOrString{intstr.FromInt(10001)}[0],
				},
			},
		},
		// Allow Prometheus monitoring access from any namespace
		{
			Ports: []networkingv1.NetworkPolicyPort{
				{
					Protocol: &[]corev1.Protocol{corev1.ProtocolTCP}[0],
					Port:     &[]intstr.IntOrString{intstr.FromInt(8080)}[0],
				},
			},
			From: []networkingv1.NetworkPolicyPeer{
				// Allow Prometheus pods from any namespace
				{
					PodSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      "app.kubernetes.io/name",
								Operator: metav1.LabelSelectorOpIn,
								Values:   []string{"prometheus", "prometheus-server", "prometheus-operator"},
							},
						},
					},
				},
				// Allow OpenShift monitoring namespace (backward compatibility)
				{
					NamespaceSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      corev1.LabelMetadataName,
								Operator: metav1.LabelSelectorOpIn,
								Values:   []string{"openshift-monitoring"},
							},
						},
					},
				},
				// Allow common monitoring namespaces
				{
					NamespaceSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      corev1.LabelMetadataName,
								Operator: metav1.LabelSelectorOpIn,
								Values:   []string{"monitoring", "prometheus", "grafana", "observability"},
							},
						},
					},
				},
			},
		},
		// Allow secured ports
		{
			Ports: allSecuredPorts,
		},
	}

	// Add RayJob submitter peer if RayCluster is owned by RayJob
	if rayJobPeer := r.buildRayJobPeer(instance); rayJobPeer != nil {
		ingressRules = append(ingressRules, networkingv1.NetworkPolicyIngressRule{
			From: []networkingv1.NetworkPolicyPeer{*rayJobPeer},
		})
	}

	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-head", instance.Name),
			Namespace: instance.Namespace,
			Labels:    labels,
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					utils.RayClusterLabelKey:  instance.Name,
					utils.RayNodeTypeLabelKey: string(rayv1.HeadNode),
				},
			},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress:     ingressRules,
		},
	}
}

// buildWorkerNetworkPolicy creates a NetworkPolicy for Ray worker pods
func (r *NetworkPolicyController) buildWorkerNetworkPolicy(instance *rayv1.RayCluster) *networkingv1.NetworkPolicy {
	labels := map[string]string{
		utils.RayClusterLabelKey:                instance.Name,
		utils.KubernetesApplicationNameLabelKey: utils.ApplicationName,
		utils.KubernetesCreatedByLabelKey:       utils.ComponentName,
	}

	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-workers", instance.Name),
			Namespace: instance.Namespace,
			Labels:    labels,
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					utils.RayClusterLabelKey:  instance.Name,
					utils.RayNodeTypeLabelKey: string(rayv1.WorkerNode),
				},
			},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{
					From: []networkingv1.NetworkPolicyPeer{
						{
							PodSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									utils.RayClusterLabelKey: instance.Name,
								},
							},
						},
					},
				},
			},
		},
	}
}

// buildRayJobPeer creates a NetworkPolicy peer for RayJob submitter pods
// Returns nil if RayCluster is not owned by RayJob
func (r *NetworkPolicyController) buildRayJobPeer(instance *rayv1.RayCluster) *networkingv1.NetworkPolicyPeer {
	// Check if RayCluster is owned by RayJob
	for _, ownerRef := range instance.OwnerReferences {
		if ownerRef.Kind == "RayJob" {
			// Return peer for RayJob submitter pods
			return &networkingv1.NetworkPolicyPeer{
				PodSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"batch.kubernetes.io/job-name": ownerRef.Name,
					},
				},
			}
		}
	}
	// No RayJob owner = no RayJob submitter pods to allow
	return nil
}

// SetupWithManager sets up the controller with the Manager
func (r *NetworkPolicyController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&rayv1.RayCluster{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Named("networkpolicy").
		Complete(r)
}
