package apiGatewayConfDeploy

import (
	"crypto/md5"
	"sort"
	"strings"
	"sync"
)


type ETagAndConfig struct {
	ETag string
	Configs []Configuration
}

func createConfigurationsEtag() *ConfigurationsEtagCache {
	return &ConfigurationsEtagCache{
		List: &SortedList{},
		mutex: &sync.RWMutex{},
		configChange: make(chan interface{}, 10),
	}
}



type ConfigurationsEtagCache struct {
	List *SortedList
	etag string
	mutex *sync.RWMutex
	configChange chan interface{}
}



func (c *ConfigurationsEtagCache) Construct(deps []Configuration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.List.Construct(deps)
	c.etag = string(c.List.ComputeHash())
	go c.notifyChange()
}

func (c *ConfigurationsEtagCache) Insert(dep *Configuration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.List.Insert(dep)
	c.etag = string(c.List.ComputeHash())
	go c.notifyChange()
}

// delete the ids from cache, and notify change
func (c *ConfigurationsEtagCache) DeleteBunch(ids []string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, id := range ids {
		c.List.Delete(id)
	}
	c.etag = string(c.List.ComputeHash())
	go c.notifyChange()
}

func (c *ConfigurationsEtagCache) GetETag() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.etag
}

func (c *ConfigurationsEtagCache) GetConfigsAndETag() *ETagAndConfig {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return &ETagAndConfig{
		ETag: c.etag,
		Configs: c.List.GetConfigs(),
	}
}

func (c *ConfigurationsEtagCache) notifyChange() {
	c.configChange <- true
}

func (c *ConfigurationsEtagCache) getChangeChannel() <- chan interface{} {
	return c.configChange
}

type SortedList struct {
	list  []*Configuration
}

func (s *SortedList) less(i int, j int) bool {
	return strings.Compare(s.list[i].ID, s.list[j].ID) < 0
}

func (s *SortedList) Insert(dep *Configuration) {
	i := s.indexOf(dep.ID)
	s.list = append(s.list, nil)
	copy(s.list[i+1:], s.list[i:])
	s.list[i] = dep
}

func (s *SortedList) ComputeHash() [16]byte {
	ids := make([]string, len(s.list))
	for i, dep := range s.list {
		ids[i] = dep.ID
	}
	return md5.Sum([]byte(strings.Join(ids, "")))
}

func (s *SortedList) Delete(id string) {
	i := s.indexOf(id)
	if s.list[i].ID == id {
		s.list = append(s.list[:i], s.list[i+1:]...)
	}
}

func (s *SortedList) indexOf(id string) int {
	var i int
	for i = 0; strings.Compare(s.list[i].ID, id) == -1; i++ {
	}
	return i
}

func (s *SortedList) Construct(l []Configuration) {

	s.list = make([]*Configuration, len(l))
	for i, dep := range l {
		s.list[i] = &dep
	}
	sort.Slice(s.list, s.less)
}

func (s *SortedList) Len() int {
	return len(s.list)
}

func (s *SortedList) GetConfigs() []Configuration {
	deps := make([]Configuration, len(s.list))
	for i, dep := range s.list {
		deps[i] = *dep
	}
	return deps
}