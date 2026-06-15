package controller

import (
	"fmt"
	"regexp"
	"strings"
)

const LabelPrefix = "simple-volume.shipstuff.io"

var labelUnsafe = regexp.MustCompile(`[^a-z0-9A-Z_.-]+`)

func VolumeLabelID(namespace, name string) string {
	id := name
	if namespace != "" {
		id = namespace + "." + name
	}
	id = strings.ToLower(id)
	id = strings.ReplaceAll(id, "/", ".")
	id = labelUnsafe.ReplaceAllString(id, "-")
	id = strings.Trim(id, "-.")
	if id == "" {
		return "volume"
	}
	if len(id) > 45 {
		id = id[:45]
		id = strings.Trim(id, "-.")
	}
	return id
}

func HealthyLabel(namespace, name string) string {
	return fmt.Sprintf("%s/%s-healthy", LabelPrefix, VolumeLabelID(namespace, name))
}

func RoleLabel(namespace, name string) string {
	return fmt.Sprintf("%s/%s-role", LabelPrefix, VolumeLabelID(namespace, name))
}

func PoolLabel(pool string) string {
	return fmt.Sprintf("%s/pool-%s", LabelPrefix, VolumeLabelID("", pool))
}
