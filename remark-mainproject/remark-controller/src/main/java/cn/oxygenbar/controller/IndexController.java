package cn.oxygenbar.controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.mvc.multiaction.MultiActionController;

import cn.oxygenbar.util.Logs;


	@Controller
	@RequestMapping(value = "test/*.do")
	public class IndexController extends MultiActionController {
		public String index(HttpServletRequest request, HttpServletResponse response) {
			request.setAttribute("test", "test");
			Logs.geterrorLogger().error("你好吗");
			return "index";
		}
	}
