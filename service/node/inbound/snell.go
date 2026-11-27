package inbound

import (
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/snell"
	CM "github.com/sagernet/sing-box/service/manager/constant"
	"github.com/sagernet/sing-box/service/node/constant"
	E "github.com/sagernet/sing/common/exceptions"
)

type SnellManager struct {
	inbounds map[string]*SnellUserManager

	mtx sync.Mutex
}

func NewSnellManager() *SnellManager {
	return &SnellManager{
		inbounds: make(map[string]*SnellUserManager),
	}
}

func (m *SnellManager) AddUserManager(inbound adapter.Inbound) error {
	in, ok := inbound.(*snell.Inbound)
	if !ok {
		return E.New("snell inbound ", inbound.Tag(), " is not a snell inbound")
	}
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.inbounds[inbound.Tag()] = &SnellUserManager{
		inbound:  in,
		usersMap: make(map[string]option.SnellUser),
	}
	return nil
}

func (m *SnellManager) GetUserManager(tag string) (constant.UserManager, bool) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	inbound, ok := m.inbounds[tag]
	return inbound, ok
}

func (m *SnellManager) GetUserManagerTags() []string {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	tags := make([]string, 0, len(m.inbounds))
	for tag := range m.inbounds {
		tags = append(tags, tag)
	}
	return tags
}

type SnellUserManager struct {
	inbound  *snell.Inbound
	usersMap map[string]option.SnellUser

	mtx sync.Mutex
}

func (i *SnellUserManager) UpdateUser(user CM.User) {
	i.mtx.Lock()
	defer i.mtx.Unlock()
	i.usersMap[user.Username] = convertSnellUser(user)
	i.postUpdate()
}

func (i *SnellUserManager) UpdateUsers(users []CM.User) {
	i.mtx.Lock()
	defer i.mtx.Unlock()
	clear(i.usersMap)
	for _, user := range users {
		i.usersMap[user.Username] = convertSnellUser(user)
	}
	i.postUpdate()
}

func (i *SnellUserManager) DeleteUser(username string) {
	i.mtx.Lock()
	defer i.mtx.Unlock()
	delete(i.usersMap, username)
	i.postUpdate()
}

// The user's password is their user key.
func convertSnellUser(user CM.User) option.SnellUser {
	return option.SnellUser{Name: user.Username, UserKey: user.Password}
}

func (i *SnellUserManager) postUpdate() {
	users := make([]option.SnellUser, 0, len(i.usersMap))
	for _, user := range i.usersMap {
		users = append(users, user)
	}
	_ = i.inbound.UpdateUsers(users)
}
