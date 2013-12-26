#include <iostream>
#include <memory>
#include <atomic>

#include <sched.hh>


#if 0
#include "server.hh"
#include "context.hh"
#endif

#include "udp.hh"


int main(int argc, char **argv)
{
    std::cerr << "OSv-Memcache version 0.1\n";

    int ncpus = sched::cpus.size();
    std::cerr << "Running on " << ncpus << " cpus.\n";

    /**** UDP Server ****/
    // Create one thread listening to UDP requests, which routes these
    // requests to one of several additional request-processing threads.
    // If we just have one CPU, there is no point in the additional thread,
    // and the listening thread processes the requests on-the-spot (they
    // cannot block).
    // Pin each thread to a different CPU - it is unlikely our scheduler's
    // load balancer can do anything smarter.
    udp_server();

    /**** TCP SERVER ****/
#if 0 /* TODO: add TCP server code */
    // TODO: perhaps start fewer than ncpus service threads? Do we want to
    // leave a CPU for virtio thread, netchannels multiplexor, etc.?
    std::vector<sched::thread *> threads;
    for (int i = 0; i < ncpus; i++) {
        // Start one thread pinned to each CPU.
        threads.push_back(new server_thread(i));
    }
    threads[0]->add(new tcp_listen_context(11211));

    for (auto *t : threads) {
        t->start();
    }

    for (auto *t : threads) {
        delete t;
    }
#endif


}
