(use-modules
 (rnrs bytevectors)
 (srfi srfi-111)
 ((core scalability)
  #:renamer (symbol-prefix-proc 'nn:)))

(define address "ipc:///tmp/pubsub.ipc")

(define (make-subscriber address topics proc timeout-ms)

  "Connect to `address`, subscribe to `topics` and evaluate `proc`,
presumably for side effects, for each received message as a
bytevector. The subscriber will disconnect if `proc` returns the empty
list. Returns a thunk which will, when evaluated, cause the subscriber
to dissconnect within `timeout-ms`."

  (define (make-socket address topics timeout)
    (let ((s (nn:pubsub-sub-socket)))
      (nn:connect s address)
      (apply nn:subscribe s topics)
      (nn:set-recv-timeout s timeout)
      s))

  (define (make-control-proc sentinel thread)
    (lambda (command)
      (case command
        ((stop) (set-box! sentinel #t))
        ((join) (join-thread thread))
        (else (format #t "SUBSCRIBER: UNKNOWN COMMAND ~s\n" command)))))

  (define (process-message s)
    (nn:call-with-recv-msg s proc))

  (let* ((sentinel (box #f))
         (thread (call-with-new-thread
                  (lambda ()
                    (let ((s (make-socket address topics timeout-ms)))
                      (let loop ((result (process-message s)))
                        (if (or (null? result) (unbox sentinel))
                            (nn:shutdown s)
                            (loop (process-message s)))))))))
    (make-control-proc sentinel thread)))

(define recieved-message-counter 0)

(define (monitor-received-message-counter)
  (call-with-new-thread
    (lambda ()
      (let loop ()
        (format #t "COUNTER: ~S\n" recieved-message-counter)
        (usleep 1000000)
        (loop)))))

(define (run-subscriber)
 (let ((subscriber-ctl
        (make-subscriber
         address '("foo|")
         (lambda (s)
           (format #t "MSG: ~s ~s\n" (utf8->string s) s)
           (set! recieved-message-counter (+ recieved-message-counter 1)))
         1000)))
   (subscriber-ctl 'join)))

;; (begin (monitor-received-message-counter)
;;        (run-subscriber))

(define (make-publisher addr)
  (let ((s (nn:pubsub-pub-socket)))
    (nn:bind s addr)
    (lambda (msg)
      (nn:send-string s msg))))
