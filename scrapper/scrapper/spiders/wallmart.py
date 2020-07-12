# -*- coding: utf-8 -*-
import scrapy


class WallmartSpider(scrapy.Spider):
    name = 'wallmart' 
    start_urls = ['https://www.walmart.com/search/?page=1&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=2&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=3&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=4&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=5&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=6&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=7&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=8&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=9&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=10&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=11&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=12&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=13&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=14&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=15&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=16&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=17&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=18&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=19&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=20&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=21&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=22&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=23&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=24&ps=40&query=phones',
                  'https://www.walmart.com/search/?page=25&ps=40&query=phones',


                  ]

    def parse(self, response):
         divs=response.css("li.u-size-1-5-xl").css("div.search-result-gridview-item-wrapper")
        
         for div in divs:
             titleText=div.css("a.line-clamp-2").css("span::text").extract()
             priceWhole=div.css("span.price-main").css("span.price-characteristic::text").extract()
             priceFraction=div.css("span.price-main").css("span.price-mantissa::text").extract()
             totalPeopleRated=div.css("span.stars-reviews-count ::text").extract()
             rating=div.css("div.stars-small").css("span.seo-avg-rating::text").extract()
             imageUrl=div.css("img").xpath("@src").extract()
             website="walmart.com"
             #yield{"page":imageUrl  }
             yield{"titleText":titleText,"priceWhole":priceWhole,"priceFraction":priceFraction,"rating":rating,"totalPeopleRated":totalPeopleRated,"imageUrl":imageUrl,"website":website}  
            
         
            #nextPage=response.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "paginator-list", " " ))]//a').extract()
         #page="https://www.walmart.com/search/?page="+str(t)+"&ps=40&query=speaker"
        # yield{"page":nextPage}
        # if pageNumber < 20:
        #    yield response.follow(page,callback=self.parse)
         pass 