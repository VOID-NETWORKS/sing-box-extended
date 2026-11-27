package inbound

import (
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/shadowsocks"
	CM "github.com/sagernet/sing-box/service/manager/constant"
	"github.com/sagernet/sing-box/service/node/constant"
	E "github.com/sagernet/sing/common/exceptions"
)

type ShadowsocksManager struct {
	inbounds map[string]*ShadowsocksUserManager

	mtx sync.Mutex
}

func NewShadowsocksManager() *ShadowsocksManager {
	return &ShadowsocksManager{
		inbounds: make(map[string]*ShadowsocksUserManager),
	}
}

func (m *ShadowsocksManager) AddUserManager(inbound adapter.Inbound) error {
	in, ok := inbound.(*shadowsocks.MultiInbound)
	if !ok {
		return E.New("shadowsocks inbound ", inbound.Tag(), " is not multi-user: start it with \"managed\": true")
	}
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.inbounds[inbound.Tag()] = &ShadowsocksUserManager{
		inbound:  in,
		usersMap: make(map[string]option.ShadowsocksUser),
	}
	return nil
}

func (m *ShadowsocksManager) GetUserManager(tag string) (constant.UserManager, bool) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	inbound, ok := m.inbounds[tag]
	return inbound, ok
}

func (m *ShadowsocksManager) GetUserManagerTags() []string {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	tags := make([]string, 0, len(m.inbounds))
	for tag := range m.inbounds {
		tags = append(tags, tag)
	}
	return tags
}

type ShadowsocksUserManager struct {
	inbound  *shadowsocks.MultiInbound
	usersMap map[string]option.ShadowsocksUser

	mtx sync.Mutex
}

func (i *ShadowsocksUserManager) UpdateUser(user CM.User) {
	i.mtx.Lock()
	defer i.mtx.Unlock()
	i.usersMap[user.Username] = convertShadowsocksUser(user)
	i.postUpdate()
}

func (i *ShadowsocksUserManager) UpdateUsers(users []CM.User) {
	i.mtx.Lock()
	defer i.mtx.Unlock()
	clear(i.usersMap)
	for _, user := range users {
		i.usersMap[user.Username] = convertShadowsocksUser(user)
	}
	i.postUpdate()
}

func (i *ShadowsocksUserManager) DeleteUser(username string) {
	i.mtx.Lock()
	defer i.mtx.Unlock()
	delete(i.usersMap, username)
	i.postUpdate()
}

// The user's password is their key: for the 2022 methods a base64 key of the
// method's length, for the older ones any string.
func convertShadowsocksUser(user CM.User) option.ShadowsocksUser {
	return option.ShadowsocksUser{Name: user.Username, Password: user.Password}
}

func (i *ShadowsocksUserManager) postUpdate() {
	names := make([]string, 0, len(i.usersMap))
	keys := make([]string, 0, len(i.usersMap))
	for _, user := range i.usersMap {
		names = append(names, user.Name)
		keys = append(keys, user.Password)
	}
	_ = i.inbound.UpdateUsers(names, keys)
}
