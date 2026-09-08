package transport

import (
	"net"
	"net/netip"
	"strings"
)

var bridgePrefixes = []string{"docker", "br-", "cni", "flannel", "veth", "virbr", "lxc", "utun", "awdl", "llw"}

func isBridgeName(name string) bool {
	for _, p := range bridgePrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// interfaceIPs returns the machine's dialable unicast addresses: up
// interfaces, no container bridges, no loopback or link-local.
func interfaceIPs() []netip.Addr {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var out []netip.Addr
	seen := map[netip.Addr]bool{}
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagUp == 0 || isBridgeName(ifc.Name) {
			continue
		}
		addrs, err := ifc.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			n, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip, ok := netip.AddrFromSlice(n.IP)
			if !ok {
				continue
			}
			ip = ip.Unmap()
			if seen[ip] || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || !ip.IsValid() || ip.IsUnspecified() {
				continue
			}
			seen[ip] = true
			out = append(out, ip)
		}
	}
	return out
}
