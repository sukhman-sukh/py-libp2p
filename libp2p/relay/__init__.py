"""Relay functionality for libp2p.

This package implements relay functionality for libp2p, including:
- Circuit Relay v2 protocol
- DCUtR (Direct Connection Upgrade through Relay) for NAT traversal

This package includes implementations of circuit relay protocols
for enabling connectivity between peers behind NATs or firewalls.
It also provides NAT traversal capabilities via Direct Connection Upgrade through Relay (DCUtR).
"""

# Import the circuit_v2 module to make it accessible
# through the relay package
from libp2p.relay.circuit_v2 import (
    PROTOCOL_ID,
    CircuitV2Protocol,
    CircuitV2Transport,
    RelayDiscovery,
    RelayLimits,
    RelayResourceManager,
    Reservation,
)

from libp2p.relay.holepunch import (

    DCUtRProtocol,
    DCUTR_PROTOCOL_ID,
    DCUtRProtocol,
    ReachabilityChecker,
    is_private_ip,
)

__all__ = [
    "CircuitV2Protocol",
    "CircuitV2Transport",
    "PROTOCOL_ID",
    "RelayDiscovery",
    "RelayLimits",
    "RelayResourceManager",
    "Reservation",
    "DCUtRProtocol",
    "DCUTR_PROTOCOL_ID",
    "ReachabilityChecker",
    "is_private_ip",
]
