#include <assert.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <string>
#include <vector>

static void msg(const char* msg){
    fprintf(stderr, "\%s", msg);
}

static void die(const char* msg){
    int err = errno;
    fprintf(stderr, "[%d] %s\n", err, msg);
    abort();
}

static int32_t read_full(int fd, uint8_t *buff, size_t n){
    while(n>0){
        ssize_t rv = read(fd, buff, n);
        
        if(rv<=0){
            return -1; // some error
        }
        
        assert(size_t(rv)<=n);
        n-=(size_t)rv;
        buff+=rv;
    }
    
    return 0;
}

static int32_t write_all(int fd, uint8_t *buff, size_t n){
    while(n>0){
        ssize_t rv = write(fd, buff, n);
        
        if(rv<=0){
            return -1; // some error
        }
        
        assert(rv<=n);
        n-=(size_t)rv;
        buff+=rv;
    }
    
    return 0;
}

static void buff_append(std::vector<uint8_t> &buff, const uint8_t *data, size_t len){
    buff.insert(buff.end(), data, data + len);
}

const size_t k_max_msg = 32 << 20;

static int32_t send_req(int fd, const uint8_t *text, size_t len){
    if (len > k_max_msg) {
        return -1;
    }
    
    std::vector<uint8_t> wbuf;
    buff_append(wbuf, (const uint8_t *)&len, 4);
    buff_append(wbuf, text, len);
    return write_all(fd, wbuf.data(), wbuf.size());
}

static int32_t read_res(int fd){
    std::vector<uint8_t> rbuf;
    rbuf.resize(4);
    errno = 0;
    
    //4 bytes are for the header
    int32_t err = read_full(fd, &rbuf[0], 4);
    
    if(err){
        if (errno == 0) {
            msg("EOF");
        } else {
            msg("read() error");
        }
        return err;
    }
    
    uint32_t len = 0;
    
    memcpy(&len, rbuf.data(), 4); //assuming little endian
    
    if (len > k_max_msg) {
        msg("too long");
        return -1;
    }
    
    rbuf.resize(4 + len);
    err = read_full(fd, &rbuf[4], len);
    if (err) {
        msg("read() error");
        return err;
    }
    
    printf("len:%u data:%.*s\n", len, len < 100 ? len : 100, &rbuf[4]);
    return 0;
}

int main(){
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if(fd<0){
        die("socket()");
    }
    
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK); //127.0.0.1
    
    int rv = connect(fd, (const struct sockaddr *)&addr, sizeof(addr));
    
    if(rv){
        die("connect");
    }
    
    // multiple pipelined requests
    std::vector<std::string> query_list ;
    query_list.push_back("hello1");
    query_list.push_back("hello2");
    query_list.push_back("hello3");
    query_list.push_back(std::string(k_max_msg, 'z'));
    query_list.push_back("hello4");
    
    for (const std::string &s : query_list) {
        int32_t err = send_req(fd, (uint8_t *)s.data(), s.size());
        if (err) {
            goto L_DONE;
        }
    }
    

L_DONE:
    close(fd);
    return 0;
}