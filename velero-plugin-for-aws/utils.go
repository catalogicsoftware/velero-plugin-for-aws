package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/ec2/types"

	"github.com/pkg/errors"
	corev1api "k8s.io/api/core/v1"
	kerror "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type PvcInfo struct {
	Namespace              string `json:"namespace,omitempty"`
	DataTransferSnapshotId string `json:"data_transfer_snapshot_id,omitempty"`
	SnapshotId             string `json:"snapshot_id,omitempty"`
	VolumeId               string `json:"volume_id,omitempty"`
	Name                   string `json:"name,omitempty"`
	PvName                 string `json:"pv_name,omitempty"`
	SnapshotType           string `json:"snapshot_type,omitempty"`
}
type PvcSnapshotProgressData struct {
	JobId            string   `json:"job_id,omitempty"`
	Message          string   `json:"message,omitempty"`
	State            string   `json:"state,omitempty"`
	SnapshotProgress int32    `json:"snapshot_progress,omitempty"`
	Pvc              *PvcInfo `json:"pvc,omitempty"`
}

const (
	TimeFormat = "2006-01-06 15:04:05 UTC: "

	CloudCasaNamespace = "cloudcasa-io"

	// Name of configmap used to to report progress of snapshot
	snapshotProgressUpdateConfigMapNamePrefix = "cloudcasa-io-snapshot-updater-aws-"
)

// UpdateSnapshotProgress updates the configmap in order to relay the
// snapshot progress to KubeAgent
func (vs *VolumeSnapshotter) UpdateSnapshotProgress(
	volumeInfo types.Volume,
	snapshotID string,
	tags map[string]string,
	percentageCompleteString string,
	state string,
	snapshotStateMessage string,
) error {
	vs.log.Info("Update Snapshot Progress - Starting to relay snapshot progress to KubAgent")
	// Fill in the PVC realted information
	var pvc = PvcInfo{}
	pvc.PvName = tags["velero.io/pv"]
	vs.log.Info("Update Snapshot Progress -", "PV Name", pvc.PvName)
	for _, tag := range volumeInfo.Tags {
		if *tag.Key == "kubernetes.io/created-for/pvc/name" {
			pvc.Name = *tag.Value
			vs.log.Info("Update Snapshot Progress -", "PVC Name", pvc.Name)
		}
		if *tag.Key == "kubernetes.io/created-for/pvc/namespace" {
			pvc.Namespace = *tag.Value
			vs.log.Info("Update Snapshot Progress -", "PVC Namespace", pvc.Namespace)
		}
	}

	switch state {
	case "completed":
		if len(snapshotStateMessage) == 0 {
			snapshotStateMessage = "EBS Snapshot Complete"
		}
	case "pending":
		if len(snapshotStateMessage) == 0 {
			snapshotStateMessage = "EBS Snapshot in Progress"
		}
	case "error":
		if len(snapshotStateMessage) == 0 {
			snapshotStateMessage = "EBS Snapshot Failed"
		}
	default:
	}

	pvc.SnapshotType = "NATIVE"
	pvc.SnapshotId = snapshotID
	pvc.VolumeId = *volumeInfo.VolumeId
	vs.log.Info("Update Snapshot Progress - ", "PVC Payload ", pvc)
	// Fill in Snapshot Progress related information
	var progress = PvcSnapshotProgressData{}
	progress.JobId = tags["velero.io/backup"]
	vs.log.Info("Update Snapshot Progress - ", "Job ID ", progress.JobId)
	currentTimeString := time.Now().UTC().Format(TimeFormat)
	progress.State = state
	progress.Message = currentTimeString + " " + snapshotStateMessage

	// Extract percentage from the string
	_, err := fmt.Sscanf(percentageCompleteString, "%d%%", &progress.SnapshotProgress)
	if err != nil {
		vs.log.Error(err, "Failed to convert percentage progress from string to int32")
	} else {
		vs.log.Info("Update Snapshot Progress - ", "Percentage: ", progress.SnapshotProgress)
	}
	progress.Pvc = &pvc
	vs.log.Info("Update Snapshot Progress - ", "Progress Payload: ", progress)

	// Prepare the paylod to be embedded into the configmap
	requestData := make(map[string][]byte)
	if requestData["snapshot_progress_payload"], err = json.Marshal(progress); err != nil {
		newErr := errors.Wrap(err, "Failed to marshal progress while creating the snapshot progress configmap")
		vs.log.Error(newErr, "JSON marshalling failed")
		return newErr
	}
	vs.log.Info("Update Snapshot Progress - Marsahlled the JSON payload")
	// create the configmap object.
	progressConfigMapName := snapshotProgressUpdateConfigMapNamePrefix + progress.JobId
	moverConfigMap := corev1api.ConfigMap{
		TypeMeta: v1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: v1.ObjectMeta{
			Name:      progressConfigMapName,
			Namespace: CloudCasaNamespace,
		},
		BinaryData: requestData,
	}
	vs.log.Info("Update Snapshot Progress - Created the configmap object")
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		newErr := errors.Wrap(err, "Failed to create in-cluster config")
		vs.log.Error(newErr, "Failed to create in-cluster config")
		return newErr

	}
	vs.log.Info("Update Snapshot Progress - Created in-cluster config")
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		newErr := errors.Wrap(err, "Failed to create clientset")
		vs.log.Error(newErr, "Failed to create clientset")
		return newErr
	}
	vs.log.Info("Update Snapshot Progress - Created clientset")
	//Create or update the configmap
	var mcm *corev1api.ConfigMap
	if _, mErr := clientset.CoreV1().ConfigMaps(CloudCasaNamespace).Get(context.TODO(), progressConfigMapName, v1.GetOptions{}); kerror.IsNotFound(mErr) {
		mcm, err = clientset.CoreV1().ConfigMaps(CloudCasaNamespace).Create(context.TODO(), &moverConfigMap, v1.CreateOptions{})
		if err != nil {
			newErr := errors.Wrap(err, "Failed to create configmap to report snapshotprogress")
			vs.log.Error(newErr, "Failed to create configmap")
			return newErr

		}
		vs.log.Info("Created configmap to report snapshot progress. ", "Configmap Name: ", mcm.GetName())
	} else {
		mcm, err = clientset.CoreV1().ConfigMaps(CloudCasaNamespace).Update(context.TODO(), &moverConfigMap, v1.UpdateOptions{})
		if err != nil {
			newErr := errors.Wrap(err, "Failed to update configmap to report snapshotprogress")
			vs.log.Error(newErr, "Failed to update configmap")
			return newErr
		}
		vs.log.Info("Updated configmap to report snapshot progress. ", "Configmap Name: ", mcm.GetName())
	}
	vs.log.Info("finished relaying snapshot progress to KubeAgent")
	return nil
}

// DeleteSnapshotProgressConfigMap deletes the configmap used to report snapshot progress
func (vs *VolumeSnapshotter) DeleteSnapshotProgressConfigMap(tags map[string]string) {
	jobId := tags["velero.io/backup"]
	progressConfigMapName := snapshotProgressUpdateConfigMapNamePrefix + jobId
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		vs.log.Error(errors.Wrap(err, "Failed to create in-cluster config"))
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		vs.log.Error(errors.Wrap(err, "Failed to create in-cluster clientset"))
	}
	err = clientset.CoreV1().ConfigMaps(CloudCasaNamespace).Delete(context.TODO(), progressConfigMapName, v1.DeleteOptions{})
	if err != nil {
		vs.log.Error(errors.Wrap(err, "Failed to delete configmap used to report snapshot progress"))
	} else {
		vs.log.Info("Deleted configmap used to report snapshot progress", "Configmap Name", progressConfigMapName)
	}
}
