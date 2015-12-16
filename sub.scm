;;; Run via:
;;; guile -l prelude.scm -l sub.scm

(use-modules (core scalability))

(define address "ipc:///tmp/pubsub.ipc")

(define (process-message s)
  (format #t "received: ~s\n" s))

(define message-count 0)

(call-with-new-thread
 (lambda ()
   (call-with-socket-endpoint
    'sub '((receive-timeout 5000))
    'bind address
    (lambda (s)
      (format #t "subscribe: ~s\n" (subscribe s "test|"))
      (let loop ((result
                  (call-with-received-string s process-message)))
        (cond (result
               (set! message-count (+ message-count 1))
               (format #t "result: ~s\n" result))
              (else
               (format #t "message-count: ~a\n" message-count)))
        (loop (call-with-received-string s process-message)))))))
