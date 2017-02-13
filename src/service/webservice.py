#!/usr/bin/env python
# -*- coding: utf-8 -*-

#########################################################################
# File Name: service/webservice.py
# Author: tangming
# mail: 707167666@qq.com
# Created Time: 2015-11-20 17:15
#########################################################################


# stdlib
import time
import signal
# tornado
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.autoreload
import tornado.options
from tornado.options import define, options
# zpb
from zpb.conf import Conf, logger
# web
from zpb.service.web.account import AccountVerifyHandler, LoginHandler, AccountBindHandler, AccountUnbindHandler
from zpb.service.web.resume import ResumeExportHandler, ResumeAckExportHandler, ResumeAnalysisHandler
from zpb.service.web.email import EmailBindHandler, EmailUnbindHandler


define('debug', default=False, help='enable debug mode')
define('port', default=Conf.HTTPPORT, help='run on the given port', type=int)
tornado.options.parse_command_line()
settings = {
    'debug': options.debug,
    'gzip': True,
    'autoescape': None,
    'xsrf_cookies': False,
    'cookie_secret': '/39zIP4w+bg='
}


def make_app():
    return tornado.web.Application([
        (r'/verify/', AccountVerifyHandler),
        (r'/account/login/', LoginHandler),
        (r'/account/bind/', AccountBindHandler),
        (r'/account/unbind/', AccountUnbindHandler),
        (r'/email/bind/', EmailBindHandler),
        (r'/email/unbind/', EmailUnbindHandler),
        (r'/resume/export/', ResumeExportHandler),
        (r'/resume/ackexport/', ResumeAckExportHandler),
        (r'/resume/analysis/', ResumeAnalysisHandler)
    ], **settings)


app = make_app()
http_server = tornado.httpserver.HTTPServer(app)


def sig_handler(signum, frame):
    logger.warning('Http Server got signal {}'.format(Conf.SIGNAL_NAMES.get(signum, 'UNKNOWN')))
    tornado.ioloop.IOLoop.instance().add_callback_from_signal(shutdown)


def shutdown():
    logger.info('Stopping Http Server')
    http_server.stop()
    logger.info('Http Server will shutdown in %s seconds ...', 60)
    io_loop = tornado.ioloop.IOLoop.instance()
    deadline = time.time() + 60
    def stoploop():
        now = time.time()
        if now < deadline and (io_loop._callbacks or io_loop._timeouts):
            io_loop.add_timeout(now + 1, stoploop)
        else:
            io_loop.stop()
            logger.info('Http Server shutdown')
    stoploop()


def RunWebApp():
    logger.info('Http Server listen at port:<{}>'.format(options.port))
    http_server.listen(options.port)
    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)
    tornado.ioloop.IOLoop.current().start()


if __name__ == '__main__':
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
    #
    RunWebApp()
