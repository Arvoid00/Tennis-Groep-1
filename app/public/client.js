// client-side js
// run by the browser each time your view template is loaded

// by default, you've got jQuery,
// add other scripts at the bottom of index.html

console.log("HI");

$("#file").on("change", function() {
  var filename = $("#file").val().split("\\").reverse()[0]
  console.log(filename)
  $("#filename").before('<br />').text(filename).append('<i class="fas fa-check" style="color:green;"></i><br />');
});