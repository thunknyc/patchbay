(define-module (core foreign)
  #:use-module (system foreign)
  #:export (lib-func))

(define (lib-func lib ret name . args)
  (pointer->procedure ret
   (dynamic-func name lib) args))
