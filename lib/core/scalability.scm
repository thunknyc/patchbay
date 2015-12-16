(define-module (core scalability)
  #:use-module (rnrs bytevectors)
  #:use-module (core)
  #:use-module (core scalability primitives)
  #:export (call-with-socket-endpoint
            send-string-to-socket
            call-with-received-string)
  #:re-export (subscribe))


(define (call-with-socket socket-type options proc)

  (define (socket-for-type)
    (case socket-type
      ((bus) (bus-socket))
      ((pair) (pair-socket))
      ((push) (pipeline-push-socket))
      ((pull) (pipeline-pull-socket))
      ((pub) (pubsub-pub-socket))
      ((sub) (pubsub-sub-socket))
      ((surveyor) (survey-surveyor-socket))
      ((respondent) (survey-respondent-socket))
      ((request) (reqrep-request-socket))
      ((reply) (reqrep-reply-socket))
      (else (throw 'unknown-socket-type socket-type))))
  
  (define (process-options s options)

    (define (set-option proc option-args)
      (let ((result (apply proc s option-args)))
        (if result result (throw 'socket-option-failed s proc option-args))))

    (map (lambda (option)
           (case (car option)
             ((linger)
              (set-option set-linger (cdr option)))
             ((receive-timeout)
              (set-option set-recv-timeout (cdr option)))
             (else
              (throw 'unknown-socket-option option))))
         options))

  (let ((s (socket-for-type)))
    (process-options s options)
    (with-throw-handler #t
      (lambda ()
        (let ((value (proc s)))
          (close s)
          value))
      (lambda (key . args)
        (close s)
        (apply throw key args)))))

(define (with-endpoint s endpoint-type addr thunk)

  (define (bind-or-connect)
    (case endpoint-type
      ((connect)
       (let ((endpoint (connect s addr)))
         (if (>= endpoint 0)
             endpoint
             (throw 'connect-failed s addr))))
      ((bind)
       (let ((endpoint (bind s addr)))
         (if (>= endpoint 0)
             endpoint
             (throw 'bind-failed s addr))))
      (else
       (throw 'unknown-endpoint-type endpoint-type))))

  (let ((endpoint (bind-or-connect)))
    (with-throw-handler #t
      (lambda ()
        (let ((value (thunk)))
          (shutdown s endpoint)
          value))
      (lambda (key . args)
        (shutdown s endpoint)
        (apply throw key args)))))

(define (call-with-socket-endpoint socket-type socket-options
                                   endpoint-type address proc)
  (call-with-socket
   socket-type socket-options
   (lambda (s)
     (with-endpoint
      s endpoint-type
      address
      (lambda ()
        (proc s))))))

(define (call-with-received-message s proc)
  (let ((msg (recv s)))
    (cond (msg
           (let ((value (proc msg)))
             (freemsg msg)
             value))
          ((eagain?)
           #f)
          (else
           (throw 'call-with-received-message-failed s (errno))))))

(define (call-with-received-string s proc)
  (call-with-received-message s
    (lambda (msg)
      (proc (utf8->string msg)))))

(define (send-to-socket s msg)
  (let ((bytes (send s msg)))
    (cond ((and (not bytes) (eagain?))
           #f)
          ((not bytes)
           (throw 'send-to-socket-failed s (errno)))
          (else
           bytes))))

(define (send-string-to-socket s str)
  (send-to-socket s (string->utf8 str)))

(comment
 (begin

   (define address "ipc:///tmp/test.ipc")

   (call-with-socket-endpoint
        'sub '() 'bind address
        (lambda (s)
          (format #t "SOCK: ~s SUBSCRIBE: ~s\n" s (subscribe s "test|"))
          (call-with-received-string
           s
           (lambda (str)
             (format #t "RECEIVED: ~s\n" str)))))

   ))

(comment

 (define address "ipc:///tmp/pubsub.ipc")

 (call-with-socket-endpoint
  'pub '() 'connect
  (lambda (s e) (list s e)))

 (call-with-new-thread
  (lambda ()
       (call-with-socket-endpoint
        'sub '() 'bind address
        (lambda (s)
          (subscribe s "test|")
          (call-with-received-string
           s
           (lambda (str)
             (format #t "RECEIVED: ~s\n" str)))))))

 (call-with-socket-endpoint
  'pub '() 'connect address
  (lambda (s)
    (send-string-to-socket s "test|hello, world."))))
