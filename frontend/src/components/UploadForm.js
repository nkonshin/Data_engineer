import React, { useState } from 'react';
import axios from 'axios';

function UploadForm() {
  const [file, setFile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [recs, setRecs] = useState(null);

  const handleFileChange = (e) => {
    setFile(e.target.files[0]);
  };

  const handleUpload = async () => {
    if (!file) return;
    setLoading(true);
    const formData = new FormData();
    formData.append('file', file);
    try {
      const response = await axios.post(`${window.__BACKEND_URL__ || 'http://localhost:8000'}/upload-file/`, formData);
      setRecs(response.data.recommendations);
      alert('Пайплайн создан, ID=' + response.data.pipeline_id);
    } catch (error) {
      console.error(error);
      alert('Ошибка при загрузке файла');
    }
    setLoading(false);
  };

  return (
    <div className="upload-form">
      <h2>Загрузить данные</h2>
      <input type="file" onChange={handleFileChange} />
      <button onClick={handleUpload} disabled={loading}>
        {loading ? 'Анализ...' : 'Анализировать и создать пайплайн'}
      </button>
      {recs && (
        <div className="recommendations">
          <h3>Рекомендации:</h3>
          <pre>{JSON.stringify(recs, null, 2)}</pre>
        </div>
      )}
    </div>
  );
}

export default UploadForm;


