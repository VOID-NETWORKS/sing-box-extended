package inbound

import (
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/mieru"
	CM "github.com/sagernet/sing-box/service/manager/constant"
	"github.com/sagernet/sing-box/service/node/constant"
	E "github.com/sagernet/sing/common/exceptions"
)

type MieruManager struct {
	inbounds map[string]*MieruUserManager

	mtx sync.Mutex
}

func NewMieruManager() *MieruManager {
	return &MieruManager{
		inbounds: make(map[string]*MieruUserManager),
	}
}

func (m *MieruManager) AddUserManager(inbound adapter.Inbound) error {
	in, ok := inbound.(*mieru.Inbound)
	if !ok {
		return E.New("mieru inbound ", inbound.Tag(), " is not a mieru inbound")
	}
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.inbounds[inbound.Tag()] = &MieruUserManager{
		inbound:  in,
		usersMap: make(map[string]option.MieruUser),
	}
	return nil
}

func (m *MieruManager) GetUserManager(tag string) (constant.UserManager, bool) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	inbound, ok := m.inbounds[tag]
	return inbound, ok
}

func (m *MieruManager) GetUserManagerTags() []string {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	tags := make([]string, 0, len(m.inbounds))
	for tag := range m.inbounds {
		tags = append(tags, tag)
	}
	return tags
}

type MieruUserManager struct {
	inbound  *mieru.Inbound
	usersMap map[string]option.MieruUser

	mtx sync.Mutex
}

func (i *MieruUserManager) UpdateUser(user CM.User) {
	i.mtx.Lock()
	defer i.mtx.Unlock()
	i.usersMap[user.Username] = convertMieruUser(user)
	i.postUpdate()
}

func (i *MieruUserManager) UpdateUsers(users []CM.User) {
	i.mtx.Lock()
	defer i.mtx.Unlock()
	clear(i.usersMap)
	for _, user := range users {
		i.usersMap[user.Username] = convertMieruUser(user)
	}
	i.postUpdate()
}

func (i *MieruUserManager) DeleteUser(username string) {
	i.mtx.Lock()
	defer i.mtx.Unlock()
	delete(i.usersMap, username)
	i.postUpdate()
}

func convertMieruUser(user CM.User) option.MieruUser {
	return option.MieruUser{Name: user.Username, Password: user.Password}
}

func (i *MieruUserManager) postUpdate() {
	users := make([]option.MieruUser, 0, len(i.usersMap))
	for _, user := range i.usersMap {
		users = append(users, user)
	}
	_ = i.inbound.UpdateUsers(users)
}
