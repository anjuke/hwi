$(function(){

	/* back to top */
    $(window).scroll(function () {
        if ($(this).scrollTop() > 400) {
            $('#back-top').fadeIn()
        } else {
            $('#back-top').fadeOut()
        }
    });

    $('#back-top').click(function () {
        $('body,html').animate({
            scrollTop: 0
        }, 800)
        return false
    });

    /* code syntax highlight */
    hljs.initHighlightingOnLoad()
    

    /* short query tooltip */
    $('.short-query.tip').tooltip({title:function(){
    	return $(this).siblings('.long-query').text()
    }})
    
    /* short query toggle */
    $('.short-query.toggle').click(function(){
    	return $(this).siblings('.long-query').toggle()
    })
});
