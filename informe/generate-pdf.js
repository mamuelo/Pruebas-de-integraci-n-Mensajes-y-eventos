const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const htmlPath = path.join(__dirname, 'informe-pruebas-integracion.html');
const pdfPath = path.join(__dirname, 'informe-pruebas-integracion.pdf');
const edgePath = 'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe';

function generatePdf() {
  if (!fs.existsSync(htmlPath)) {
    console.error(`[ERROR] No se encontró el archivo HTML en: ${htmlPath}`);
    process.exit(1);
  }

  console.log(`[INFO] Compilando informe técnico a PDF con Microsoft Edge Headless...`);
  console.log(`Origen HTML: ${htmlPath}`);
  console.log(`Destino PDF: ${pdfPath}`);

  const fileUrl = 'file:///' + htmlPath.replace(/\\/g, '/');
  const cmd = `"${edgePath}" --headless --disable-gpu --run-all-compositor-stages-before-draw --no-pdf-header-footer "--print-to-pdf=${pdfPath}" "${fileUrl}"`;

  try {
    execSync(cmd, { stdio: 'pipe', timeout: 30000 });
    // Pequeña espera para asegurar sincronización de escritura en disco
    const maxWait = 10;
    let waited = 0;
    while (!fs.existsSync(pdfPath) && waited < maxWait) {
      execSync('ping -n 2 127.0.0.1 > nul');
      waited++;
    }

    if (fs.existsSync(pdfPath)) {
      const stats = fs.statSync(pdfPath);
      console.log(`[SUCCESS] PDF generado exitosamente! Tamaño: ${(stats.size / 1024).toFixed(2)} KB`);
    } else {
      console.error(`[ERROR] El archivo PDF no se encontró tras la ejecución del motor.`);
    }
  } catch (err) {
    console.error(`[ERROR] Fallo al generar PDF: ${err.message}`);
  }
}

if (require.main === module) {
  generatePdf();
}

module.exports = { generatePdf };
