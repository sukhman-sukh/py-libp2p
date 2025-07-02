"""
Direct Connection Upgrade through Relay (DCUtR) protocol implementation.

This module implements the DCUtR protocol as specified in:
https://github.com/libp2p/specs/blob/master/relay/DCUtR.md

DCUtR enables peers behind NAT to establish direct connections
using hole punching techniques.

# Note: For better clarity, function docstrings include examples illustrating peer relationships.
# Here A started the relayed connection with B but B started the holepunching process. 
"""

import logging
from typing import (
    Any,
)

from multiaddr import (
    Multiaddr,
)
import trio

from libp2p.abc import (
    IHost,
    INetStream,
)
from libp2p.custom_types import (
    TProtocol,
)
from libp2p.peer.id import (
    ID,
)
from libp2p.tools.async_service import (
    Service,
)
from .nat import (
    is_addr_relayed, 
    is_addr_public
)
from libp2p.relay.holepunch.pb import dcutr_pb2

from enum import Enum, auto

logger = logging.getLogger("libp2p.relay.holepunch.dcutr")

# Protocol ID for DCUtR
PROTOCOL_ID = TProtocol("/libp2p/dcutr")

# Timeout constants
DIAL_TIMEOUT = 15  # seconds
SYNC_TIMEOUT = 5  # seconds
HOLE_PUNCH_TIMEOUT = 30  # seconds

# Maximum number of retries for holepunching
Max_HOLE_PUNCHING_RETRIES = 3 

# Maximum observed addresses to exchange
MAX_OBSERVED_ADDRS = 20

# Maximum message size (4KiB as per spec)
MAX_MESSAGE_SIZE = 4 * 1024



class HolePunchState(Enum):
    SUCCESS = auto()
    ALREADY_CONNECTED = auto()
    ALREADY_IN_PROGRESS = auto()
    STREAM_OPEN_FAILED = auto()
    SEND_CONNECT_FAILED = auto()
    RECEIVE_CONNECT_FAILED = auto()
    SYNC_FAILED = auto()
    HOLE_PUNCH_TIMEOUT = auto()
    DIAL_FAILED = auto()
    NO_VALID_ADDRS = auto()
    UNKNOWN_ERROR = auto()

class DCUtRProtocol(Service):
    """
    DCUtRProtocol implements the Direct Connection Upgrade through Relay protocol.

    This protocol allows two NATed peers to establish direct connections through
    hole punching, after they have established an initial connection through a relay.
    """

    def __init__(self, host: IHost):
        """
        Initialize the DCUtR protocol.

        Parameters
        ----------
        host : IHost
            The libp2p host this protocol is running on
        """
        super().__init__()
        self.host = host
        self.event_started = trio.Event()
        self._hole_punch_attempts: dict[ID, int] = {}
        # DOUBT: Right now I am considering this direct_connection as already holepunched direct connection and not unilateral direct connection.  
        # self._direct_connections: set[ID] = set()
        # self._in_progress: set[ID] = set()
        self._connection_state: dict[ID, HolePunchState] = {}

    async def run(self, *, task_status: Any = trio.TASK_STATUS_IGNORED) -> None:
        """Run the protocol service."""
        # TODO: Implement the service run method that:
        try:
            # Register the DCUtR protocol handlers
            logger.debug("Registering stream handlers for DCUtR protocol ")
            self.host.set_stream_handler(PROTOCOL_ID, self._handle_dcutr_stream)
            logger.debug("Stream handlers registered successfully")
            
            self.event_started.set()
            task_status.started()
            logger.debug("Protocol service started")
            
        finally:
            try:
                pass
                # host_with_handlers = cast(IHostWithStreamHandlers, self.host)
                # host_with_handlers.remove_stream_handler(PROTOCOL_ID)
                # host_with_handlers.remove_stream_handler(STOP_PROTOCOL_ID)
            except Exception as e:
                logger.error("Error unregistering stream handlers: %s", str(e))
        # 3. Waits for the service to be stopped
        # 4. Unregisters the protocol handler on shutdown

    async def _handle_dcutr_stream(self, stream: INetStream) -> None:
        """
        Handle incoming DCUtR streams.
        (DCUtR stream handler in A)
        Parameters
        ----------
        stream : INetStream
            The incoming stream
        """
        # TODO: Implement the stream handler that:
        # 1. Gets the remote peer ID
        remote_peer_id = stream.muxed_conn.peer_id
        # 2. Checks if there's already an active hole punch attempt
        if self._connection_state[remote_peer_id] == HolePunchState.ALREADY_IN_PROGRESS:
            logger.debug("Holepunching already in progress with peer_id: ", remote_peer_id)
            return
        # 3. Checks if we already have a direct connection
        if self._connection_state[remote_peer_id] == HolePunchState.ALREADY_CONNECTED:
            logger.debug("Already connected to peer with peer_id: ", remote_peer_id)
        # 4. Reads and parses the initial CONNECT message
        resp_bytes = await stream.read(MAX_MESSAGE_SIZE)
        resp = dcutr_pb2.HolePunch()
        resp.ParseFromString(resp_bytes)
        
        # 5. Processes observed addresses from the peer
        remote_obs_addr = [addr.decode("utf-8") for addr in resp.ObsAddrs]
        
        # 6. Sends our CONNECT message with our observed addresses
        obs_addrs_bytes = self._get_observed_addrs()
        # Prepare the DCUtR protobuf message with type CONNECT and observed addresses
        msg = dcutr_pb2.HolePunch()
        msg.type = dcutr_pb2.HolePunch.CONNECT
        msg.ObsAddrs.extend(obs_addrs_bytes)
        msg_bytes = msg.SerializeToString()
        if len(msg_bytes) > MAX_MESSAGE_SIZE:
            logger.debug("DCUtR message too large to send")
            self._connection_state[remote_peer_id] = HolePunchState.SEND_CONNECT_FAILED
            return 
        
        # 7. Handles the SYNC message for hole punching coordination
        
        # 8. Performs the hole punch attempt


# Tests to implement for this function 
# 1. No holepunching for already hole punched. 
# 2. NO holepunching if other peer can be directly connected. 
# 3. No holepunching if it is already in progress
    async def initiate_hole_punch(self, peer_id: ID) -> HolePunchState:
        """
        Initiate a hole punch with a peer.
        (From B -> A)

        Parameters
        ----------
        peer_id : ID
            The peer to hole punch with

        Returns
        -------
        HolePunchState
            State indicating the result or error of the hole punch attempt
        """
        # TODO: Implement the hole punch initiation that:
        
        # Checks if we already have a direct connection via hole punching
        if self._connection_state[peer_id] == HolePunchState.ALREADY_CONNECTED:
            return HolePunchState.ALREADY_CONNECTED
        
        # Check if the peer has any non-relayed, public addresses to attempt UnilateralConnectionUpgrade 
        peer_addrs = self.host.get_peerstore().peer_info(peer_id).addrs
        public_addrs = [addr for addr in peer_addrs if is_addr_relayed(addr) and is_addr_public(addr)]
        if public_addrs and await self.attemptUnilateralConnectionUpgrade(peer_id, public_addrs):
            return HolePunchState.ALREADY_CONNECTED
        
        
        # Checks if there's already an active hole punch attempt
        if self._connection_state[peer_id] == HolePunchState.ALREADY_IN_PROGRESS:
            return HolePunchState.ALREADY_IN_PROGRESS
        
        for retries in Max_HOLE_PUNCHING_RETRIES:
            try:
                # Opens a DCUtR stream to the peer
                connection = self.host.get_network().connections.get(peer_id)
                dcutr_stream = await connection.new_stream(PROTOCOL_ID)
                obs_addrs_bytes = self._get_observed_addrs()
                
                # Prepare the DCUtR protobuf message with type CONNECT and observed addresses
                msg = dcutr_pb2.HolePunch()
                msg.type = dcutr_pb2.HolePunch.CONNECT
                msg.ObsAddrs.extend(obs_addrs_bytes)
                # INSERT_YOUR_CODE
                # Serialize the protobuf message
                msg_bytes = msg.SerializeToString()
                if len(msg_bytes) > MAX_MESSAGE_SIZE:
                    logger.debug("DCUtR message too large to send")
                    return HolePunchState.SEND_CONNECT_FAILED
                
                rtt_start_time = trio.current_time()
                try:
                    # await trio.to_thread.run_sync(dcutr_stream.write, msg_bytes)
                    # await trio.to_thread.run_sync(dcutr_stream.send_eof)
                    dcutr_stream.write()
                except Exception as e:
                    logger.debug(f"Failed to send DCUtR CONNECT message: {e}")
                    return HolePunchState.SEND_CONNECT_FAILED
                
                break
            except Exception as e:
                logger.debug(f"Failed to initiate hole punching to the given peer.")
                continue
                
        # 4. Sends a CONNECT message with our observed addresses
        # 5. Receives the peer's CONNECT message
        # 6. Calculates the RTT for synchronization
        # 7. Sends a SYNC message with timing information
        # 8. Performs the synchronized hole punch
        # 9. Verifies the direct connection
        return HolePunchState.UNKNOWN_ERROR

    async def _dial_peer(self, peer_id: ID, addr: Multiaddr) -> None:
        """
        Attempt to dial a peer at a specific address.

        Parameters
        ----------
        peer_id : ID
            The peer to dial
        addr : Multiaddr
            The address to dial
        """
        # TODO: Implement the peer dialing logic that:
        # 1. Attempts to connect to the peer at the given address
        # 2. Handles timeouts and connection errors
        # 3. Updates connection tracking if successful

    async def _have_direct_connection(self, peer_id: ID) -> bool:
        """
        Check if we already have a direct connection to a peer.

        Parameters
        ----------
        peer_id : ID
            The peer to check

        Returns
        -------
        bool
            True if we have a direct connection, False otherwise
        """
        # TODO: Implement the direct connection check that:
        
        # Peer is already in direct connection to out host 
        if self._connection_state[peer_id] == HolePunchState.ALREADY_CONNECTED:
            return True
        
        if peer_id in self.host.get_connected_peers():
            peer_addr: list[Multiaddr] = self.host.get_peerstore().peer_info(peer_id).addrs
            non_relayed_addrs = [addr for addr in peer_addr if not is_addr_relayed(addr)]
        # 3. If connected, verifies it's a direct connection (not relayed)
        # 4. Updates our direct connections set if needed
        return False

    async def _get_observed_addrs(self) -> list[bytes]:
        """
        Get our observed addresses to share with the peer.

        Returns
        -------
        List[bytes]
            List of observed addresses as bytes
        """
        # TODO: Implement the observed address collection that:
        # 1. Gets our listen addresses from the host
        # 2. Filters and limits the addresses according to the spec
        # 3. Converts addresses to the required format
        return []

    def _decode_observed_addrs(self, addr_bytes: list[bytes]) -> list[Multiaddr]:
        """
        Decode observed addresses received from a peer.

        Parameters
        ----------
        addr_bytes : List[bytes]
            The encoded addresses

        Returns
        -------
        List[Multiaddr]
            The decoded multiaddresses
        """
        # TODO: Implement the address decoding logic that:
        # 1. Converts bytes to Multiaddr objects
        # 2. Filters invalid addresses
        # 3. Returns the valid addresses
        return []
    
    async def attemptUnilateralConnectionUpgrade(self, peer_id: ID, public_addr_list: list[Multiaddr]) -> bool:
        """
        Attempt to establish a direct connection to the peer using the provided public addresses.

        Parameters
        ----------
        peer_id : ID
            The peer ID to connect to.
        public_addr_list : list[Multiaddr]
            List of public multiaddrs to try.

        Returns
        -------
        bool
            True if a direct connection was established, False otherwise.
        """
        for addr in public_addr_list:
            try:
                await self.host.connect_addr(peer_id, addr)
                return True
            except Exception as e:
                logger.debug(f"Failed to connect to {peer_id} at {addr}: {e}")
                continue
        return False