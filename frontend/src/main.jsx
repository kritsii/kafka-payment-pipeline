import React from 'react'
import { createRoot } from 'react-dom/client'
import { CssBaseline, ThemeProvider, createTheme } from '@mui/material'
import App from './App'
import './styles.css'

const theme = createTheme({
  palette: { mode: 'light', primary: { main: '#155EEF' }, background: { default: '#F5F7FA' } },
  typography: { fontFamily: '"DM Sans", "Helvetica Neue", sans-serif' },
  shape: { borderRadius: 10 },
})

createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <ThemeProvider theme={theme}><CssBaseline /><App /></ThemeProvider>
  </React.StrictMode>,
)
