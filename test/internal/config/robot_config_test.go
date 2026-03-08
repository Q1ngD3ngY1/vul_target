package config

import (
	"testing"

	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"github.com/stretchr/testify/assert"
)

func TestSearchVector_MarshalToString(t *testing.T) {
	v, err := ParseSearchVectorFromPB(nil).MarshalToString()
	assert.Nil(t, err)
	assert.Equal(t, "", v)
}

func TestRobotModel_MarshalToString(t *testing.T) {
	v, err := ParseRobotModelFromPB(nil).MarshalToString()
	assert.Nil(t, err)
	assert.Equal(t, "", v)

	v, err = ParseRobotModelFromPB(make(map[string]*pb.RobotModelInfo)).MarshalToString()
	assert.Nil(t, err)
	assert.Equal(t, "", v)
}

func TestRobotFilters_MarshalToString(t *testing.T) {
	v, err := ParseRobotFiltersFromPB(nil).MarshalToString()
	assert.Nil(t, err)
	assert.Equal(t, "", v)

	v, err = ParseRobotFiltersFromPB(make(map[string]*pb.RobotFilters)).MarshalToString()
	assert.Nil(t, err)
	assert.Equal(t, "", v)
}

func TestRobotDocSplit_MarshalToString(t *testing.T) {
	v, err := ParseRobotDocSplitFromPB(nil).MarshalToString()
	assert.Nil(t, err)
	assert.Equal(t, "", v)

	v, err = ParseRobotDocSplitFromPB(make(map[string]*pb.RobotSplitDoc)).MarshalToString()
	assert.Nil(t, err)
	assert.Equal(t, "", v)
}
