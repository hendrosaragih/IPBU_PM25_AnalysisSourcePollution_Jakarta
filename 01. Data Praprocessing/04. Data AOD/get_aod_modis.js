// =======================================================
// SCRIPT FINAL: Data AOD Harian Lengkap Jakarta Tahun 2024
// FIX: Menghapus error "remove is not a function" dengan menggunakan 'select'
// =======================================================

// ✅ 1. Tentukan periode waktu yang benar
var tanggalMulai = '2024-01-01';
var tanggalSelesai = '2024-12-31';

// Definisi Lokasi (Menambahkan lon/lat sebagai properti eksplisit)
var lokasi = ee.FeatureCollection([
  ee.Feature(ee.Geometry.Point([106.8235, -6.19466]), {name: 'bundaran_hi', lon: 106.8235, lat: -6.19466}),
  ee.Feature(ee.Geometry.Point([106.910887, -6.1535777]), {name: 'kelapa_gading', lon: 106.910887, lat: -6.1535777}),
  ee.Feature(ee.Geometry.Point([106.80367, -6.35693]), {name: 'jagakarsa', lon: 106.80367, lat: -6.35693}),
  ee.Feature(ee.Geometry.Point([106.90919, -6.28889]), {name: 'lubang_buaya', lon: 106.90919, lat: -6.28889}),
  ee.Feature(ee.Geometry.Point([106.753187, -6.207349]), {name: 'kebun_jeruk', lon: 106.753187, lat: -6.207349})
]);

// --- Bagian 1: Ekstrak data AOD yang valid (dari data yang ada) ---
var dataset = ee.ImageCollection('MODIS/061/MCD19A2_GRANULES')
        .select('Optical_Depth_047')
        .filterDate(tanggalMulai, tanggalSelesai); 

var extractAOD = function(image) {
  var date = image.date().format('YYYY-MM-dd');
  var sampled = image.reduceRegions({
    collection: lokasi, 
    reducer: ee.Reducer.mean(), 
    scale: 5000 
  });
  
  var withAOD = sampled.map(function(f) {
    var rawAOD = f.get('mean');
    var scaledAOD = ee.Algorithms.If(rawAOD, ee.Number(rawAOD).multiply(0.001), null);
    
    var name = f.get('name');
    var matchingLocation = lokasi.filter(ee.Filter.eq('name', name)).first();
    var lon = matchingLocation.get('lon');
    var lat = matchingLocation.get('lat');
    
    // Set semua properti yang relevan
    return f.set('date', date, 'AOD', scaledAOD, 'lon', lon, 'lat', lat);
  });
  
  return withAOD; 
};

var dataAODyangAda = dataset.map(extractAOD).flatten();

// --- Bagian 2: Siapkan Join untuk membuat tabel harian yang lengkap ---
var dataAODdenganKunci = dataAODyangAda.map(function(f) {
  var kunci = ee.String(f.get('date')).cat('_').cat(ee.String(f.get('name')));
  return f.set('kunci_join', kunci).select(['kunci_join', 'AOD']);
});

// Buat kerangka lengkap
var nDays = ee.Date(tanggalSelesai).difference(ee.Date(tanggalMulai), 'day').add(1);
var daftarTanggal = ee.List.sequence(0, nDays.subtract(1)).map(function(n) {
  return ee.Date(tanggalMulai).advance(n, 'day').format('YYYY-MM-dd');
});

var kerangkaLengkap = lokasi.map(function(stasiun) {
  var lon = stasiun.get('lon'); 
  var lat = stasiun.get('lat'); 
  
  var featuresForStation = daftarTanggal.map(function(tanggal) {
    var kunci = ee.String(tanggal).cat('_').cat(ee.String(stasiun.get('name')));
    return stasiun.set({
      'date': tanggal, 'kunci_join': kunci, 'lon': lon, 'lat': lat
    }).select(['date', 'name', 'lon', 'lat', 'kunci_join']);
  });
  return ee.FeatureCollection(featuresForStation);
}).flatten();

// Lakukan Join
var filterJoin = ee.Filter.equals({leftField: 'kunci_join', rightField: 'kunci_join'});
var join = ee.Join.saveAll({matchesKey: 'data_aod'});
var hasilJoin = join.apply(kerangkaLengkap, dataAODdenganKunci, filterJoin);


// --- Bagian 3: Proses Hasil Join dan Finalisasi ---
var dataFinal = hasilJoin.map(function(f) {
  var daftarCocok = ee.List(f.get('data_aod'));
  var aodValue = ee.Algorithms.If(
    daftarCocok.size().gt(0), 
    ee.Feature(daftarCocok.get(0)).get('AOD'), 
    null 
  );
  
  // FIX: Kita hanya perlu set AOD. Kolom kunci_join dan data_aod akan diabaikan oleh select() di bagian akhir.
  return f.set('AOD', aodValue);
});


// --- Bagian 4: Tampilkan dan Ekspor ---
// Menggunakan select() untuk memastikan hanya kolom yang diinginkan (termasuk lon/lat) yang diekspor
var output = dataFinal.select(['date', 'name', 'lon', 'lat', 'AOD']);
var totalBaris = 366 * 5; // 1830 baris

// Batasi output di konsol agar tidak memicu error memori
print('✅ Data Harian Lengkap untuk Tahun 2024 (10 baris uji):', output.limit(10)); 
print('✅ Total Baris yang akan diekspor (seharusnya):', totalBaris); 

// Jalankan Export di tab Tasks
Export.table.toDrive({
  collection: output,
  description: 'Jakarta_AOD_Harian_Lengkap_2024',
  fileFormat: 'CSV'
});