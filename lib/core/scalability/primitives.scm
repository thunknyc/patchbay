(define-module (core scalability primitives)
  #:use-module (system foreign)
  #:use-module (rnrs bytevectors)
  #:use-module (core combinators)
  #:use-module (core foreign)
  #:replace (socket bind connect shutdown recv send close)
  #:export (bus-socket
            pair-socket
            pipeline-push-socket pipeline-pull-socket
            pubsub-pub-socket pubsub-sub-socket
            survey-respondent-socket survey-surveyor-socket
            reqrep-request-socket reqrep-reply-socket
            set-recv-timeout set-linger
            set-surveyor-deadline
            set-reqrep-reply-resend-interval
            subscribe subscribe*
            unsubscribe
            shutdown
            send-string
            freemsg
            errno eagain?))

(define nanomsg-func (partial lib-func (dynamic-link "libnanomsg")))

(define EAGAIN 35)
(define ETIMEDOUT 60)

(define AF_SP 1)

(define NN_SOL_SOCKET 0)

;;; For `setsockopt`
(define NN_LINGER 1)
(define NN_SNDBUF 2)
(define NN_RCVBUF 3)
(define NN_SNDTIMEO 4)
(define NN_RCVTIMEO 5)
(define NN_RECONNECT_IVL 6)
(define NN_RECONNECT_IVL_MAX 7)
(define NN_SNDPRIO 8)
(define NN_RCVPRIO 9)
(define NN_SNDFD 10)
(define NN_RCVFD 11)
(define NN_DOMAIN 12)
(define NN_PROTOCOL 13)
(define NN_IPV4ONLY 14)
(define NN_SOCKET_NAME 15)
(define NN_RCVMAXSIZE 16)

(define NN_MSG -1)

(define NN_PROTO_PAIR 1)
(define NN_PAIR (+ (* NN_PROTO_PAIR 16) 0))

(define NN_PROTO_PUBSUB 2)
(define NN_PUB (+ (* NN_PROTO_PUBSUB 16) 0))
(define NN_SUB (+ (* NN_PROTO_PUBSUB 16) 1))
(define NN_SUB_SUBSCRIBE 1)
(define NN_SUB_UNSUBSCRIBE 2)

(define NN_PROTO_REQREP 3)
(define NN_REQ (+ (* NN_PROTO_REQREP 16) 0))
(define NN_REP (+ (* NN_PROTO_REQREP 16) 1))
(define NN_REQ_RESEND_IVL 1)

(define NN_PROTO_PIPELINE 5)
(define NN_PUSH (+ (* NN_PROTO_PIPELINE 16) 0))
(define NN_PULL (+ (* NN_PROTO_PIPELINE 16) 1))

(define NN_PROTO_SURVEY 6)
(define NN_SURVEYOR (+ (* NN_PROTO_SURVEY 16) 2))
(define NN_RESPONDENT (+ (* NN_PROTO_SURVEY 16) 3))
(define NN_SURVEYOR_DEADLINE 1)

(define NN_PROTO_BUS 7)
(define NN_BUS (+ (* NN_PROTO_BUS 16) 0))

(define (make-null-pointer)
  (bytevector->pointer (make-bytevector (sizeof size_t) 0)))

;;; int nn_setsockopt (int s, int level, int option,
;;;                    const void *optval, size_t optvallen);
(define nn-setsockopt (nanomsg-func int "nn_setsockopt"
                                    int int int '* size_t))

(define (setsockopt s level option opt-val-bytevector)
  (if (null? opt-val-bytevector)
      (zero? (nn-setsockopt
              s level option (dereference-pointer (make-null-pointer)) 0))
      (let* ((opt-val-ptr (bytevector->pointer opt-val-bytevector))
             (opt-val-len (bytevector-length opt-val-bytevector)))
        (zero? (nn-setsockopt s level option opt-val-ptr opt-val-len)))))

(define (setsockopt-string s level option value)
  (let ((v (string->utf8 value)))
    (setsockopt s level option v)))

(define (setsockopt-int s level option value)
  (let ((v (make-bytevector (sizeof int))))
    (bytevector-s32-native-set! v 0 value)
    (setsockopt s level option v)))

(define (subscribe-1 s topic)
  (setsockopt-string s NN_SUB NN_SUB_SUBSCRIBE topic))

(define (subscribe s . topics)
  (let loop ((topics topics))
    (cond ((null? topics) #t)
          ((subscribe-1 s (car topics))
           (loop (cdr topics)))
          (else #f))))

(define (subscribe* s topics)
  (apply subscribe s topics))

(define (unsubscribe-1 s topic)
  (setsockopt-string s NN_SUB NN_SUB_UNSUBSCRIBE topic))

(define (unsubscribe s . topics)
  (let loop ((topics topics))
    (cond ((null? topics) #t)
          ((unsubscribe-1 s (car topics))
           (loop (cdr topics)))
          (else #f))))

(define (set-linger s millis)
  (setsockopt-int s NN_SOL_SOCKET NN_LINGER millis))

(define (set-recv-timeout s millis)
  (setsockopt-int s NN_SOL_SOCKET NN_RCVTIMEO millis))

(define (set-surveyor-deadline s millis)
  (setsockopt-int s NN_SURVEYOR NN_SURVEYOR_DEADLINE millis))

(define (set-reqrep-reply-resend-interval s millis)
  (setsockopt-int s NN_PROTO_REQREP NN_REQ_RESEND_IVL millis))

;;; int nn_socket (int domain, int protocol);
(define nn-socket (nanomsg-func int "nn_socket" int int))

(define (socket domain protocol)
  (let ((sock (nn-socket domain protocol)))
    (if (>= sock 0)
        sock
        #f)))

(define (pipeline-push-socket)
  (socket AF_SP NN_PUSH))

(define (pipeline-pull-socket)
  (socket AF_SP NN_PULL))

(define (pubsub-pub-socket)
  (socket AF_SP NN_PUB))

(define (pubsub-sub-socket)
  (socket AF_SP NN_SUB))

(define (bus-socket)
  (socket AF_SP NN_BUS))

(define (survey-surveyor-socket)
  (socket AF_SP NN_SURVEYOR))

(define (survey-respondent-socket)
  (socket AF_SP NN_RESPONDENT))

(define (pair-socket)
  (socket AF_SP NN_PAIR))

(define (reqrep-request-socket)
  (socket AF_SP NN_REQ))

(define (reqrep-reply-socket)
  (socket AF_SP NN_REP))

; int nn_bind (int s, const char *addr);
(define nn-bind (nanomsg-func int "nn_bind" int '*))

(define (bind s addr)
  (nn-bind s (string->pointer addr)))

; int nn_connect (int s, const char *addr);
(define nn-connect (nanomsg-func int "nn_connect" int '*))

(define (connect s addr)
  (nn-connect s (string->pointer addr)))

; int nn_shutdown (int s, int how);
(define nn-shutdown (nanomsg-func int "nn_shutdown" int int))

(define (shutdown s how)
  (>= (nn-shutdown s how) 0))

; int nn_close (int s);
(define nn-close (nanomsg-func int "nn_close" int))

(define (close s)
  (>= (nn-close s) 0))

; int nn_send (int s, const void *buf, size_t len, int flags);
(define nn-send (nanomsg-func int "nn_send" int '* size_t int))

(define (send s msg)
  (let* ((len (bytevector-length msg))
         (bytes (nn-send s (bytevector->pointer msg) len 0)))
    (if (>= bytes 0)
        bytes
        #f)))

(define (send-string s msg)
  (send s (string->utf8 msg)))

; int nn_recv (int s, void *buf, size_t len, int flags);
(define nn-recv (nanomsg-func int "nn_recv" int '* ssize_t int))

; int nn_freemsg (void *msg);
(define nn-freemsg (nanomsg-func int "nn_freemsg" '*))

(define (recv s)
  (let* ((msg-ptr (make-null-pointer))
         (bytes (nn-recv s msg-ptr NN_MSG 0)))
    (if (>= bytes 0)
        (pointer->bytevector (dereference-pointer msg-ptr) bytes)
        #f)))

(define (freemsg msg)
  (nn-freemsg (bytevector->pointer msg)))

;;; int nn_errno (void);
(define nn-errno (nanomsg-func int "nn_errno"))

(define errno nn-errno)

(define (eagain?)
  (let ((err (errno)))
    (or (= err EAGAIN)
        (= err ETIMEDOUT))))
