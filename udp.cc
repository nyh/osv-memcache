#include <sys/types.h>
#include <sys/socket.h>
#include <stdio.h>
#include <strings.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <assert.h>
#include <time.h>
#include <string.h>

#include <iostream>
#include <unordered_map>

#include <osv/types.h>


// Open a UDP socket listening on INADDR_ANY:port.
// TODO: replace this socket file-descriptor by a lower level OSv-specific
// abstraction - i.e., an input channel from the netchannels multiplexor.
static int listening_udp_socket(int port)
{
    int fd;
    if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0){
        perror("open listening socket");
        exit(1);
    };

    struct sockaddr_in sa;
    bzero(&sa, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = INADDR_ANY;
    sa.sin_port = htons(port);
    if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        perror("bind listen_socket");
        close(fd);
        exit(1);
    }

    return fd;
}

// The first 8 bytes of each UDP memcached request is the following header,
// composed of four 16-bit integers in network byte order:
struct memcached_header {
    // request_id is an opaque value that the server needs to echo back
    // to the client.
    u16 request_id;
    // If the request or response spans n datagrams, number_of_datagrams
    // contains n, and sequence_number goes from 0 to n-1.
    // Memcached does not currently support multi-datagram requests, so
    // neither do we have to. Memcached does support multi-datagram responses,
    // but the documentation suggest that TCP is more suitable for this
    // use case anyway.
    u16 sequence_number_n;
    u16 number_of_datagrams_n;
    // Reserved for future use, should be 0
    u16 reserved;
};

// TODO: Obviously, move the hash table to a different source file. Make it a class.
typedef std::string memcache_key; // NOTE: protocol specifies limited to 250 bytes, no spaces or control chars.

struct memcache_value {
    std::string data;
    // "flags" is an opaque 32-bit integer which the clients gives in the
    // "set" command, and is echoed back on "get" commands.
    u32 flags;
    time_t exptime;
};
typedef std::string memcache_key;
std::unordered_map<memcache_key, memcache_value> cache;


static void send(int fd, struct sockaddr_in &remote_addr, char *rbuf, size_t len)
{
    sendto(fd, rbuf, len, 0, (struct sockaddr *)&remote_addr, sizeof(remote_addr));
}

static void send_msg(int fd, struct sockaddr_in &remote_addr, const char *msg, size_t msglen, char *rbuf, size_t rbuf_len)
{
    assert(rbuf_len >= sizeof(memcached_header)+msglen);
    strcpy(rbuf + sizeof(memcached_header), msg);
    send(fd, remote_addr, rbuf, sizeof(memcached_header)+msglen);
}
static void send_cmd_error(int fd, struct sockaddr_in &remote_addr, char *rbuf, size_t rbuf_len)
{
    return send_msg(fd, remote_addr, "ERROR\r\n", 7, rbuf, rbuf_len);
}
static void send_cmd_stored(int fd, struct sockaddr_in &remote_addr, char *rbuf, size_t rbuf_len)
{
    return send_msg(fd, remote_addr, "STORED\r\n", 8, rbuf, rbuf_len);
}


// TODO: can avoid rbuf if we use scatter-gather on send.
static void process_request(const char* packet, size_t len,
        char *rbuf, size_t rbuf_len, int sendfd, struct sockaddr_in &remote_addr)
{
    static const auto htons_1 = htons(1);

    auto *header = (memcached_header *) packet;
    auto *rheader = (memcached_header *) rbuf;
    if (len < sizeof(*header)) {
        // Cannot send reply, have no sequence number to reply to..
        std::cerr << "unknown packet format\n";
        return;
    }
    len -= sizeof(*header);
    packet += sizeof(*header);
    assert (rbuf_len >= sizeof(*rheader));
    rheader->request_id = header->request_id;
    rheader->sequence_number_n = 0;
    rheader->number_of_datagrams_n = htons_1;
    rheader->reserved = 0;

    // TODO: consider a more efficient parser...
    const char *p = packet, *pastend = packet + len;
    while (*p != ' ' && *p != '\r' && p < pastend) {
        p++;
    }
    auto n = p - packet;
    if (p == pastend) {
        send_cmd_error(sendfd, remote_addr, rbuf, rbuf_len);
        return;
    }
    if (n == 3) {
        if(!strncmp(packet, "get", 3)) {
            std::cerr<<"got 'get'\n";
        } else if(!strncmp(packet, "set", 3)) {
            std::cerr<<"got 'set'\n";
            long flags, exptime, bytes;
            int end;
            char key[251];
            auto z =
              sscanf(packet+4, "%250s %ld %ld %ld%n", &key, &flags, &exptime, &bytes, &end);
            if (z != 4) {
                send_cmd_error(sendfd, remote_addr, rbuf, rbuf_len);
                return;
            }
            // TODO: check if there is "noreply" at 'end'
            if (len < 4 + end + 2 + bytes) {
                std::cerr << "got too small packet ?! len="<<len<<", end="<<end<<", bytes="<<bytes<<"\n";
                send_cmd_error(sendfd, remote_addr, rbuf, rbuf_len);
                return;
            }
            cache[std::string(key)] = { std::string(packet + 4 + end + 2, bytes), (u32)flags, exptime};
            send_cmd_stored(sendfd, remote_addr, rbuf, rbuf_len);
            std::cerr<<"got set with " << bytes << " bytes\n";
            return;
        }
    }

    std::cerr<<"Error... Got "<<packet<<"\n";

    send_cmd_error(sendfd, remote_addr, rbuf, rbuf_len);
}

void udp_server()
{
    int fd = listening_udp_socket(11211);

    char buf[1<<16];
    char rbuf[1<<16];

    for (;;) {
        struct sockaddr_in remote_addr;
        socklen_t len = sizeof(remote_addr);
        auto n = recvfrom(fd, buf, sizeof(buf) ,0, (struct sockaddr *) &remote_addr, &len);
        std::cerr << "got packet.\n";
        // TODO: check if n<0 and abort...
        // TODO: don't send repsonses on the same fd we read on!!! The locks are completely unecessary.
        process_request(buf, n, rbuf, sizeof(rbuf), fd, remote_addr);
        //sendto(sockfd,mesg,n,0,(struct sockaddr *)&cliaddr,sizeof(cliaddr));
    }
}



