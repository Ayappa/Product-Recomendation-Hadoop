# -*- coding: utf-8 -*-
import scrapy


class AmazonSpider(scrapy.Spider):
    name = 'amazon'
    start_urls = ['https://www.amazon.com/s?k=ipod']

    def parse(self, response):
        
        divs=response.css("div.s-border-bottom")
        for div in divs:
            titleText=div.css("h2.a-color-base").css("span.a-text-normal::text").extract()
            priceWhole=div.css("span.a-price-whole::text").extract()
            priceFraction=div.css("span.a-price-fraction::text").extract()
            rating=div.css("span.a-icon-alt::text").extract()
            totalPeopleRated=div.css("a.a-link-normal").css("span.a-size-base::text").extract()
            imageUrl=div.css("a.a-link-normal").css("img").xpath("@src").extract()
            website="amazon.com"
            yield{"titleText":titleText,"priceWhole":priceWhole,"priceFraction":priceFraction,"rating":rating,"totalPeopleRated":totalPeopleRated,"imageUrl":imageUrl,"website":website}

        
        nextPage=response.css("li.a-last").css("a::attr(href)").get()
        if nextPage is not None:
            yield response.follow(nextPage,callback=self.parse)
            
        pass
