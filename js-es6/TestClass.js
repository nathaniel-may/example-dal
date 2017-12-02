// var val;

// class TraceLogger{

//   	constructor(val){
//   		this.val = val.
//   	}

//   	customFn(input){
//   		console.out(input + ' : ' + val);
//   		return input;
//   	}

// }

class Point {
  constructor(x, y) {
    this.x = x;
    this.y = y;
  }

  static distance(a, b) {
    const dx = a.x - b.x;
    const dy = a.y - b.y;

    return Math.hypot(dx, dy);
  }
}

module.exports = Point;