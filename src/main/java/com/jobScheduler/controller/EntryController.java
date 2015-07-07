package com.jobScheduler.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class EntryController {

	@RequestMapping("/home")
	public String homePage(){
		
		System.out.println("Enterd...!!!!!");
		return "index";
	}
	
	@RequestMapping("/home1")
	public String homePage1(){
		
		System.out.println("Enterd...!!!!!");
		return "static/index";
	}
	
	@RequestMapping("/home2")
	public String homePage2(){
		
		System.out.println("Enterd...!!!!!");
		return "template/index";
	}
}
