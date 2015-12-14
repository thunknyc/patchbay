(define-module (core scalability)
  #:use-module (rnrs bytevectors)
  #:use-module (core)
  #:use-module (core scalability primitives))

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
    (map (lambda (option)
           (case (car option)
             ((recieve-timeout) (set-recv-timeout s (cadr option)))
             (else (throw 'unknown-socket-option option))))
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

(define (call-with-endpoint s endpoint-type addr proc)
  (let ((endpoint (case endpoint-type
                    ((connect) (connect s addr))
                    ((bind) (bind s addr))
                    (else (throw 'unknown-endpoint-type endpoint-type)))))
    (with-throw-handler #t
      (lambda ()
        (let ((value (proc endpoint)))
          (shutdown endpoint)
          value))
      (lambda (key . args)
        (shutdown endpoint)
        (apply throw key args)))))

(define (call-with-socket-endpoint socket-type socket-options
                                   endpoint-type address proc)
  (call-with-socket
   socket-type socket-options
   (lambda (s)
     (call-with-endpoint
      s endpoint-type
      address
      (lambda (e)
        (proc s e))))))

(define (call-with-recv-msg e proc)
  (let ((msg (recv e)))
    (cond (msg
           (let ((value (proc msg)))
             (freemsg msg)
             value))
          ((eagain?)
           #f)
          (else
           (throw 'call-with-recv-msg-failed e (errno))))))

(define (call-with-recv-string e proc)
  (call-with-recv-msg e
    (lambda (msg)
    (proc (utf8->string msg)))))

(define (send-to-endpoint e msg)
  (let ((bytes (send e msg)))
    (cond ((>= bytes 0)
           bytes)
          ((eagain?)
           #f)
          (throw 'send-to-endpoint-failed e (errno)))))

(define (send-string-to-endpoint e s)
  (send-to-endpoint e (string->utf8 s)))

(comment
 (call-with-socket-endpoint
  'pub '() 'connect "ipc:///tmp/test.ipc"
  (lambda (s e) (list s e))))
