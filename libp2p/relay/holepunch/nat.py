"""
NAT detection and reachability assessment for libp2p.

This module provides utilities for determining NAT status and
address reachability for peers.
"""

import logging
from typing import (
    Optional,
)

from multiaddr import (
    Multiaddr,
)

from libp2p.abc import (
    IHost,
)
from libp2p.peer.id import (
    ID,
)

logger = logging.getLogger("libp2p.relay.holepunch.nat")

# Timeout for reachability checks
REACHABILITY_TIMEOUT = 10  # seconds

# Private IP address ranges (RFC 1918)
PRIVATE_IP_RANGES = [
    ("10.0.0.0", "10.255.255.255"),  # 10.0.0.0/8
    ("172.16.0.0", "172.31.255.255"),  # 172.16.0.0/12
    ("192.168.0.0", "192.168.255.255"),  # 192.168.0.0/16
]

# Link-local address range (RFC 3927)
LINK_LOCAL_RANGE = ("169.254.0.0", "169.254.255.255")  # 169.254.0.0/16

# Loopback address range
LOOPBACK_RANGE = ("127.0.0.0", "127.255.255.255")  # 127.0.0.0/8


def ip_to_int(ip: str) -> int:
    """
    Convert an IP address to an integer.

    Parameters
    ----------
    ip : str
        IP address to convert

    Returns
    -------
    int
        Integer representation of the IP
    """
    octets = ip.split(".")
    return (
        (int(octets[0]) << 24)
        + (int(octets[1]) << 16)
        + (int(octets[2]) << 8)
        + int(octets[3])
    )


def is_ip_in_range(ip: str, start_range: str, end_range: str) -> bool:
    """
    Check if an IP address is within a range.

    Parameters
    ----------
    ip : str
        IP address to check
    start_range : str
        Start of IP range
    end_range : str
        End of IP range

    Returns
    -------
    bool
        True if IP is in range
    """
    ip_int = ip_to_int(ip)
    start_int = ip_to_int(start_range)
    end_int = ip_to_int(end_range)
    return start_int <= ip_int <= end_int


def is_private_ip(ip: str) -> bool:
    """
    Check if an IP address is private.

    Parameters
    ----------
    ip : str
        IP address to check

    Returns
    -------
    bool
        True if IP is private
    """
    for start_range, end_range in PRIVATE_IP_RANGES:
        if is_ip_in_range(ip, start_range, end_range):
            return True

    # Check for link-local addresses
    if is_ip_in_range(ip, *LINK_LOCAL_RANGE):
        return True

    # Check for loopback addresses
    if is_ip_in_range(ip, *LOOPBACK_RANGE):
        return True

    return False


def extract_ip_from_multiaddr(addr: Multiaddr) -> Optional[str]:
    """
    Extract the IP address from a multiaddr.

    Parameters
    ----------
    addr : Multiaddr
        Multiaddr to extract from

    Returns
    -------
    Optional[str]
        IP address or None if not found
    """
    # Convert to string representation
    addr_str = str(addr)

    # Look for IPv4 address
    ipv4_start = addr_str.find("/ip4/")
    if ipv4_start != -1:
        # Extract the IPv4 address
        ipv4_end = addr_str.find("/", ipv4_start + 5)
        if ipv4_end != -1:
            return addr_str[ipv4_start + 5 : ipv4_end]

    # Look for IPv6 address
    ipv6_start = addr_str.find("/ip6/")
    if ipv6_start != -1:
        # Extract the IPv6 address
        ipv6_end = addr_str.find("/", ipv6_start + 5)
        if ipv6_end != -1:
            return addr_str[ipv6_start + 5 : ipv6_end]

    return None


# class ReachabilityChecker:
#     """
#     Utility class for checking peer reachability.

#     This class assesses whether a peer's addresses are likely
#     to be directly reachable or behind NAT.
#     """

#     def __init__(self, host: IHost):
#         """
#         Initialize the reachability checker.

#         Parameters
#         ----------
#         host : IHost
#             The libp2p host
#         """
#         self.host = host
#         self._peer_reachability: dict[ID, bool] = {}
#         self._known_public_peers: set[ID] = set()

def is_addr_public(addr: Multiaddr) -> bool:
    """
    Check if an address is likely to be publicly reachable.

    Parameters
    ----------
    addr : Multiaddr
        The multiaddr to check

    Returns
    -------
    bool
        True if address is likely public
    """
    # Extract the IP address
    ip = extract_ip_from_multiaddr(addr)
    if not ip:
        return False

    # Check if it's a private IP
    return not is_private_ip(ip)

def get_public_addrs(addrs: list[Multiaddr]) -> list[Multiaddr]:
    """
    Filter a list of addresses to only include likely public ones.

    Parameters
    ----------
    addrs : List[Multiaddr]
        List of addresses to filter

    Returns
    -------
    List[Multiaddr]
        List of likely public addresses
    """
    return [addr for addr in addrs if is_addr_public(addr)]

def is_addr_relayed(addr: Multiaddr) -> bool:
    """
    Check if a multiaddr is a relayed address (i.e., uses /p2p-circuit).

    Parameters
    ----------
    addr : Multiaddr
        The multiaddr to check

    Returns
    -------
    bool
        True if the address is relayed (contains /p2p-circuit), False otherwise
    """
    # Convert to string and check for /p2p-circuit in the multiaddr
    return "/p2p-circuit" in str(addr)

