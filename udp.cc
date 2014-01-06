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

// Copied from bsd/sys/sys/param.h. Probably should appear in just one place
// TODO: also need little endian version!!
#define __bswap16_const(_x) (__uint16_t)((_x) << 8 | (_x) >> 8)
static constexpr inline u_int16_t __htons(u_int16_t x) { return __bswap16_const(x); }
static constexpr inline u_int16_t __ntohs(u_int16_t x) { return __bswap16_const(x); }


// The first 8 bytes of each UDP memcached request is the following header,
// composed of four 16-bit integers in network byte order:
class memcached_header {
private:
    // request_id is an opaque value that the server needs to echo back
    // to the client.
    u16 request_id;
    // If the request or response spans n datagrams, number_of_datagrams
    // contains n, and sequence_number goes from 0 to n-1.
    // Memcached does not currently support multi-datagram requests, so
    // neither do we have to. Memcached does support multi-datagram responses,
    // but the documentation suggest that TCP is more suitable for this
    // use case anyway, so we don't support this case as well.
    // This means we can always reuse a request header as the response header!
    u16 sequence_number_n;
    u16 number_of_datagrams_n;
    // Reserved for future use, should be 0
    u16 reserved;

    static constexpr auto htons_1 = __htons(1);
public:
    explicit memcached_header(u16 rid) :
            request_id(rid), sequence_number_n(0),
            number_of_datagrams_n(htons_1), reserved(0) {}
    inline bool invalid() const {
        return sequence_number_n != 0 ||
                number_of_datagrams_n != htons_1;
        // Could have also checked reserved !=0, but memaslap actually sets it to 1...
    }
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

//template<class First, class... Rest> void do_to_iovec(iovec &iov, int offset, First first, Rest... rest)
//{
//    iov[offset] = first;
//    to_iovec(iov, offset+1, rest)
//}
//template<class First> void do_to_iovec(iovec &iov, int offset, First first)
//{
//    iov[offset] = first;
//}
//
//
//template<class... Types> iovec &&to_iovec(Types... args)
//{
//    iovec iov[sizeof...(Types)];
//    do_to_iovec(iov, 0, args...);
//    return iov;
//}


static inline void send(int fd, const struct sockaddr_in &remote_addr, iovec *iov, size_t iovlen) {
    msghdr msg;
    msg.msg_iov = iov;
    msg.msg_iovlen = iovlen;
    msg.msg_name = const_cast<void*>((const void*)&remote_addr);
    msg.msg_namelen = sizeof(remote_addr);
    msg.msg_control = 0;
    msg.msg_controllen = 0;
    sendmsg(fd, &msg, 0);
}

static inline void send(int fd, const struct sockaddr_in &remote_addr,
        const memcached_header &header,
        const char *body, size_t bodylen)
{
    // The msghdr type predates "const". sendmsg will not write to any of the
    // pointers we give it.
    iovec iov[] = {
            { const_cast<void*>((const void*)&header), sizeof(header) },
            { const_cast<void*>((const void*)body), bodylen },
    };
    send(fd, remote_addr, iov, sizeof(iov) / sizeof(iov[0]));
}

static inline void send(int fd, const struct sockaddr_in &remote_addr,
        const memcached_header &header,
        const char *body1, size_t bodylen1,
        const char *body2, size_t bodylen2)
{
    iovec iov[] = {
            { const_cast<void*>((const void*)&header), sizeof(header) },
            { const_cast<void*>((const void*)body1), bodylen1 },
            { const_cast<void*>((const void*)body2), bodylen2 },
    };
    send(fd, remote_addr, iov, sizeof(iov) / sizeof(iov[0]));
}

static inline void send(int fd, const struct sockaddr_in &remote_addr,
        const memcached_header &header,
        const char *body1, size_t bodylen1,
        const char *body2, size_t bodylen2,
        const char *body3, size_t bodylen3)
{
    iovec iov[] = {
            { const_cast<void*>((const void*)&header), sizeof(header) },
            { const_cast<void*>((const void*)body1), bodylen1 },
            { const_cast<void*>((const void*)body2), bodylen2 },
            { const_cast<void*>((const void*)body3), bodylen3 },
    };
    send(fd, remote_addr, iov, sizeof(iov) / sizeof(iov[0]));
}


static void send_cmd_error(int fd, const struct sockaddr_in &remote_addr,
        const memcached_header &header)
{
    constexpr static char msg[] = "ERROR\r\n";
    send(fd, remote_addr, header, msg, sizeof(msg)-1);
}
static void send_cmd_stored(int fd, const struct sockaddr_in &remote_addr,
        const memcached_header &header)
{
    constexpr static char msg[] = "STORED\r\n";
    send(fd, remote_addr, header, msg, sizeof(msg)-1);
}
static void send_cmd_end(int fd, const struct sockaddr_in &remote_addr,
        const memcached_header &header)
{
    constexpr static char msg[] = "END\r\n";
    send(fd, remote_addr, header, msg, sizeof(msg)-1);
}


static void process_request(const char* packet, size_t len,
        int sendfd, struct sockaddr_in &remote_addr)
{
    auto &header = *(memcached_header *) packet;
    if (len < sizeof(header) || header.invalid()) {
        // Cannot send reply, have no sequence number to reply to..
        std::cerr << "unknown packet format. len=" << len << "\n";
        return;
    }
    len -= sizeof(header);
    packet += sizeof(header);

    // TODO: consider a more efficient parser...
    const char *p = packet, *pastend = packet + len;
    while (*p != ' ' && *p != '\r' && p < pastend) {
        p++;
    }
    auto n = p - packet;
    if (p == pastend) {
        send_cmd_error(sendfd, remote_addr, header);
        return;
    }
    if (n == 3) {
        if(!strncmp(packet, "get", 3)) {
            //std::cerr<<"got 'get'\n";
            char key[251];
            // TODO: do this in a loop to support multiple keys on one command
            auto z = sscanf(packet+4, "%250s", &key);
            if (z != 1) {
                send_cmd_error(sendfd, remote_addr, header);
                return;
            }
            // TODO: do we need to copy the string just for find??? Need to be able to search without copy... HOW?
            auto it = cache.find(std::string(key));
            if (it == cache.end()) {
                send_cmd_end(sendfd, remote_addr, header);
                return;
            } else {
                //std::cerr << "found\n";
                char reply[sizeof(key)+128] = "VALUE "; // TODO: make length less ugly
                char *r = reply + 6;
                strcpy(r, key);
                r += strlen(key); // do we have this already?
                r += sprintf(r, " %ld %d\r\n", it->second.flags, it->second.data.length());
                constexpr static char msg[] = "\r\nEND\r\n";
                send(sendfd, remote_addr,
                        header,
                        reply, r - reply,
                        it->second.data.c_str(), it->second.data.length(),
                        msg, sizeof(msg) - 1);
                return;
            }
        } else if(!strncmp(packet, "set", 3)) {
            //std::cerr<<"got 'set'\n";
            long flags, exptime, bytes;
            int end;
            char key[251];
            auto z =
              sscanf(packet+4, "%250s %ld %ld %ld%n", &key, &flags, &exptime, &bytes, &end);
            if (z != 4) {
                send_cmd_error(sendfd, remote_addr, header);
                return;
            }
            // TODO: check if there is "noreply" at 'end'
            if (len < 4 + end + 2 + bytes) {
                std::cerr << "got too small packet ?! len="<<len<<", end="<<end<<", bytes="<<bytes<<"\n";
                send_cmd_error(sendfd, remote_addr, header);
                return;
            }
            cache[std::string(key)] = { std::string(packet + 4 + end + 2, bytes), (u32)flags, exptime};
            send_cmd_stored(sendfd, remote_addr, header);
            //std::cerr<<"got set with " << bytes << " bytes\n";
            return;
        }
    }

    std::cerr<<"Error... Got "<<packet<<"\n";

    send_cmd_error(sendfd, remote_addr, header);
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
        //std::cerr << "got packet.\n";
        // TODO: check if n<0 and abort...
        // TODO: don't send repsonses on the same fd we read on!!! The locks are completely unecessary.
        process_request(buf, n, fd, remote_addr);
        //sendto(sockfd,mesg,n,0,(struct sockaddr *)&cliaddr,sizeof(cliaddr));
    }
}



