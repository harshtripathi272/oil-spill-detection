import { useState, useCallback, useRef, DragEvent, ChangeEvent } from 'react'

export interface UploadState {
  file: File | null
  preview: string | null
  isDragging: boolean
  isUploading: boolean
  progress: number
  error: string | null
}

const ACCEPTED_TYPES = ['image/tiff', 'image/png', 'image/jpeg', 'image/jpg', 'image/webp']
const MAX_SIZE = 50 * 1024 * 1024 // 50MB

export function useFileUpload() {
  const [state, setState] = useState<UploadState>({
    file: null,
    preview: null,
    isDragging: false,
    isUploading: false,
    progress: 0,
    error: null,
  })

  const inputRef = useRef<HTMLInputElement | null>(null)

  const validateFile = (file: File): string | null => {
    if (!ACCEPTED_TYPES.includes(file.type) && !file.name.endsWith('.tif') && !file.name.endsWith('.tiff')) {
      return 'Unsupported file type. Please upload GeoTIFF, PNG, or JPEG.'
    }
    if (file.size > MAX_SIZE) {
      return 'File exceeds 50MB limit.'
    }
    return null
  }

  const processFile = useCallback((file: File) => {
    const error = validateFile(file)
    if (error) {
      setState(prev => ({ ...prev, error, file: null, preview: null }))
      return
    }

    const reader = new FileReader()
    reader.onload = (e) => {
      setState(prev => ({
        ...prev,
        file,
        preview: e.target?.result as string,
        error: null,
      }))
    }
    reader.readAsDataURL(file)
  }, [])

  const onDragOver = useCallback((e: DragEvent) => {
    e.preventDefault()
    setState(prev => ({ ...prev, isDragging: true }))
  }, [])

  const onDragLeave = useCallback((e: DragEvent) => {
    e.preventDefault()
    setState(prev => ({ ...prev, isDragging: false }))
  }, [])

  const onDrop = useCallback((e: DragEvent) => {
    e.preventDefault()
    setState(prev => ({ ...prev, isDragging: false }))
    const file = e.dataTransfer.files[0]
    if (file) processFile(file)
  }, [processFile])

  const onFileSelect = useCallback((e: ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (file) processFile(file)
  }, [processFile])

  const triggerFileInput = useCallback(() => {
    inputRef.current?.click()
  }, [])

  const reset = useCallback(() => {
    setState({
      file: null,
      preview: null,
      isDragging: false,
      isUploading: false,
      progress: 0,
      error: null,
    })
  }, [])

  const simulateUpload = useCallback(() => {
    setState(prev => ({ ...prev, isUploading: true, progress: 0 }))
    let progress = 0
    const interval = setInterval(() => {
      progress += Math.random() * 15
      if (progress >= 100) {
        progress = 100
        clearInterval(interval)
        setTimeout(() => {
          setState(prev => ({ ...prev, isUploading: false, progress: 100 }))
        }, 300)
      }
      setState(prev => ({ ...prev, progress: Math.min(progress, 100) }))
    }, 200)
  }, [])

  return {
    ...state,
    inputRef,
    onDragOver,
    onDragLeave,
    onDrop,
    onFileSelect,
    triggerFileInput,
    reset,
    simulateUpload,
  }
}
