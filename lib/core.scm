(define-module (core)
  #:export (-> ->> comment doto))

(define-syntax ->
  (syntax-rules ()
    ((-> VALUE)
     VALUE)
    ((-> VALUE (PROC ARGS ...) FORMS ...)
     (-> (PROC VALUE ARGS ...) FORMS ...))))

(define-syntax ->>
  (syntax-rules ()
    ((->> VALUE)
     VALUE)
    ((->> VALUE (PROC ARGS ...) FORMS ...)
     (->> (PROC ARGS ... VALUE) FORMS ...))))

(define-syntax doto
  (syntax-rules ()
    ((doto VALUE)
     VALUE)
    ((doto VALUE (PROC ARGS ...) FORMS ...)
     (let ((x VALUE))
       (PROC x ARGS ...)
       (doto x FORMS ...)))))

(define-syntax comment
  (syntax-rules ()
    ((comment VALUES ...)
     (begin))))
