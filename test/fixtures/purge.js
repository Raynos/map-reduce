var EndStream = require("end-stream")

module.exports = function (db, cb) {
    db.keyStream().pipe(EndStream(function (key, cb) {
        db.del(key, cb)
    })).on("finish", cb)
}
