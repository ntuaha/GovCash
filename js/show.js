



var COLOR_1 = "#1f77b4";

var COLOR_2 = "#d62728";

//var X_DATA_PARSE = vida.string;

//var Y_DATA_PARSE = vida.number;

//var Y_DATA_FORMAT = d3.format("");



var groups = [];

var makeBar = function(width, height, bar_data) {
  var Y_DATA_FORMAT = d3.format("");
  
  var Y_AXIS_LABEL = bar_data.unit;
  
  if (bar_data.unit === 'percentage') {
    Y_DATA_FORMAT = d3.format(".1%");
  }
  
  var x = d3.scale.ordinal()
    .rangeRoundBands([0, width], 0.1);

  var y = d3.scale.linear()
      .range([height, 0]);
  
  var xAxis = d3.svg.axis()
      .scale(x)
      .orient("bottom");
  
  var yAxis = d3.svg.axis()
      .scale(y)
      .orient("left")
      .tickFormat(Y_DATA_FORMAT);
  
  var value_data = _.map(groups, function(d) {
    return {x_axis: d, y_axis: bar_data[d]};
  });
  
  x.domain(value_data.map(function(d) { return d.x_axis; }));
  y.domain([0, d3.max(value_data, function(d) { return d.y_axis; })]);

  var svg = d3.select("#canvas-svg").append("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
    .append("g")
      .attr("transform", "translate(" + margin.left + "," + margin.top + ")");
      
  var detailBox = svg.append("svg:text")
      .attr("dx", "20px")
      .attr("dy", "-5px")
      .attr("text-anchor", "right")
      .style("fill", "#1D5096")
      .style("font-weight", "bold");

  var title = svg.append("text")
      .attr("x", 0)
      .attr("y", -50)
      .attr("class","chart-title")
      .text(bar_data.chart_title);

  svg.append("g")
      .attr("class", "x axis")
      .attr("transform", "translate(0," + height + ")")
      .call(xAxis);

  svg.append("g")
      .attr("class", "y axis")
      .call(yAxis)
    .append("text")
      .attr("transform", "rotate(0)")
      .attr("y", -25)
      .attr("x", 0)
      .style("text-anchor", "left")
      .text(Y_AXIS_LABEL);

  svg.selectAll(".bar")
      .data(value_data)
    .enter().append("rect")
      .style("fill", function(d) {
        if (d.x_axis === groups[0]) {
          return COLOR_1;
        } else {
          return COLOR_2;
        }
      })
      .attr("x", function(d) { return x(d.x_axis); })
      .attr("width", x.rangeBand())
      .attr("y", function(d) { return y(d.y_axis); })
      .attr("height", function(d) { return height - y(d.y_axis); })
      .on("mouseover", function(d, i, j) {
        detailBox.attr("x", x.range()[i] - Y_DATA_FORMAT(d.y_axis).length / 2)
          .attr("y", y(d.y_axis))
          .text(Y_DATA_FORMAT(d.y_axis))
          .style("visibility", "visible");
      
        d3.select(this)
          .style("opacity", 0.7);
      }).on("mouseout", function() {
        detailBox.style("visibility", "hidden");
        
        d3.select(this)
          .style("opacity", 1.0);
      });
};
$(function(){


 
  d3.json("./data/pay_and_rec.json",function(data){


     var WIDTH = $("canvas-svg").width();



      var margin = {top: 70, right: 20, bottom: 30, left: 60},
          width = WIDTH - margin.left - margin.right,
          height = WIDTH - margin.top - margin.bottom;
      var width = width / data.length - 10;



      width = width > 180 ? width : 180;



    
    var keys = Object.keys(data[0]);
    for (var i = 0; i < keys.length; i++) {
      if (keys[i] !== "chart_title" && keys[i] !== "unit") {
        groups.push(keys[i]);
      }
    }

    for (i = 0; i < data.length; i++) {
      makeBar(width, width, data[i]);
    }
  });







});
