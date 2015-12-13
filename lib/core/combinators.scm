(define-module (core combinators)
  #:export (identity partial constantly))

(define (identity x) x)

(define (partial f x)
  (lambda args
    (apply f x args)))

(define (constantly x) (lambda args x))
