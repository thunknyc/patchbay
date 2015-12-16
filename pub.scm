;;; Run via:
;;; guile -l prelude.scm -l pub.scm

(use-modules (core scalability))

(define address "ipc:///tmp/pubsub.ipc")

(define (pub)
  (call-with-socket-endpoint
   'pub '((linger 5000)) 'connect address
   (lambda (s)
     (map
      (lambda (x)
        (send-string-to-socket s (format #f "test|hello, world (~a)" x)))
      (iota 10000)))))

(pub)
