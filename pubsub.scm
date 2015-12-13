(use-modules
 ((rnrs bytevectors))
 ((core scalability)
  #:renamer (symbol-prefix-proc 'nn:)))

(define address "ipc:///tmp/pubsub.ipc")

(define (make-subscriber address topics proc timeout-ms)
  "Connect to `address`, subscribe to `topics` and evaluate `proc`,
presumably for side effects, for each received message as a
bytevector. The subscriber will disconnect if `proc` returns the empty
list. Returns a thunk which will, when evaluated, cause the subscriber
to dissconnect within `timeout-ms`."
  (let* ((sentinel #f)
         (thread
          (call-with-new-thread
           (lambda ()
             (let ((s (nn:pubsub-sub-socket)))
               (nn:connect s address)
               (apply nn:subscribe s topics)
               (nn:set-recv-timeout s timeout-ms)
               (let loop ((result (nn:call-with-recv-msg s proc)))
                 (if (or (null? result) sentinel)
                     (nn:shutdown s)
                     (loop (nn:call-with-recv-msg s proc)))))))))
    (lambda (command)
      (case command
        ((stop) (set! sentinel #t))
        ((join) (join-thread thread))
        (else (format #t "SUBSCRIBER: UNKNOWN COMMAND ~s\n" command))))))

(define (make-publisher addr)
  (let ((s (nn:pubsub-pub-socket)))
    (nn:bind s addr)
    (lambda (msg)
      (nn:send-string s msg))))

;; (define p (make-publisher address))
;; (p "foo|bar")

(define counter 0)

(define (run-subscriber)
 (let ((subscriber-ctl
        (make-subscriber
         address '("foo|")
         (lambda (s)
           (format #t "MSG: ~s ~s\n" (utf8->string s) s)
           (set! counter (+ counter 1)))
         1000)))
   (call-with-new-thread
    (lambda ()
      (let loop ()
        (format #t "COUNTER: ~S\n" counter)
        (usleep 1000000)
        (loop))))
   (subscriber-ctl 'join)))


